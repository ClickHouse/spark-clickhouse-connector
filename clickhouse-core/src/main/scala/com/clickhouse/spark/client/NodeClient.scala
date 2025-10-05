/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.spark.client

import com.clickhouse.client._
import com.clickhouse.client.api.{Client, ServerException}
import com.clickhouse.client.api.enums.Protocol
import com.clickhouse.client.api.insert.{InsertResponse, InsertSettings}
import com.clickhouse.client.api.query.{QueryResponse, QuerySettings}
import com.clickhouse.data.ClickHouseFormat
import com.clickhouse.spark.Logging
import java.util.concurrent.TimeUnit
import com.clickhouse.spark.exception.{CHClientException, CHException, CHServerException}
import com.clickhouse.spark.format.{JSONCompactEachRowWithNamesAndTypesSimpleOutput, JSONEachRowSimpleOutput, NamesAndTypes, SimpleOutput}
import com.clickhouse.spark.spec.NodeSpec
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

import java.io.{ByteArrayInputStream, InputStream}
import java.util.UUID
import scala.util.{Failure, Success, Try}

object NodeClient {
  def apply(node: NodeSpec): NodeClient = new NodeClient(node)
}

class NodeClient(val nodeSpec: NodeSpec) extends AutoCloseable with Logging {
  // TODO: add configurable timeout
  private val timeout: Int = 30000

  private lazy val userAgent = {
    val title = getClass.getPackage.getImplementationTitle
    val version = getClass.getPackage.getImplementationVersion
    if (version != null && title != null) {
      val versions = version.split("_")
      if (versions.length < 3) {
        "Spark-ClickHouse-Connector"
      } else {
        val sparkVersion = versions(0)
        val scalaVersion = versions(1)
        val connectorVersion = versions(2)
        s"${title}/${connectorVersion} (fv:spark/${sparkVersion}, lv:scala/${scalaVersion})"
      }
    } else {
      "Spark-ClickHouse-Connector"
    }
  }

  private val clientV2 = new Client.Builder()
    .setUsername(nodeSpec.username)
    .setPassword(nodeSpec.password)
    .setDefaultDatabase(nodeSpec.database)
    .setOptions(nodeSpec.options)
    .setClientName(userAgent)
    .addEndpoint(Protocol.HTTP, nodeSpec.host, nodeSpec.port, false) // TODO: get s full URL instead
    .build()

  override def close(): Unit = {
    clientV2.close()
  }

  private def nextQueryId(): String = UUID.randomUUID.toString

  // //////////////////////////////////////////////////////////////////////////////
  // ///////////////////////// Synchronized Normal API ////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def syncQueryOutputJSONEachRow(
    sql: String,
    settings: Map[String, String] = Map.empty
  ): Either[CHException, SimpleOutput[ObjectNode]] =
    syncQuery(sql, "JSONEachRow", JSONEachRowSimpleOutput.deserialize, settings)

  def syncQueryAndCheckOutputJSONEachRow(
    sql: String,
    settings: Map[String, String] = Map.empty
  ): SimpleOutput[ObjectNode] =
    syncQueryAndCheck(sql, "JSONEachRow", JSONEachRowSimpleOutput.deserialize, settings)

  def syncInsertOutputJSONEachRow(
    database: String,
    table: String,
    inputFormat: String,
    data: InputStream,
    settings: Map[String, String] = Map.empty
  ): Either[CHException, SimpleOutput[ObjectNode]] =
    syncInsert(
      database,
      table,
      inputFormat,
      data,
      "JSONEachRow",
      JSONEachRowSimpleOutput.deserialize,
      settings
    )

  def syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(
    sql: String,
    settings: Map[String, String] = Map.empty
  ): SimpleOutput[Array[JsonNode]] with NamesAndTypes =
    syncQueryAndCheck(
      sql,
      "JSONCompactEachRowWithNamesAndTypes",
      JSONCompactEachRowWithNamesAndTypesSimpleOutput.deserialize,
      settings
    ).asInstanceOf[SimpleOutput[Array[JsonNode]] with NamesAndTypes]

  def syncInsert[OUT](
    database: String,
    table: String,
    inputFormat: String,
    data: InputStream,
    outputFormat: String,
    deserializer: InputStream => SimpleOutput[OUT],
    settings: Map[String, String]
  ): Either[CHException, SimpleOutput[OUT]] = {
    val queryId = nextQueryId()
    val sql = s"INSERT INTO `$database`.`$table` FORMAT $inputFormat"
    onExecuteQuery(queryId, sql)

    val insertSettings : InsertSettings = new InsertSettings();
    settings.foreach { case (k, v) => insertSettings.setOption(k, v) }
    insertSettings.setDatabase(database)
    // TODO: check what type of compression is supported by the client v2
    insertSettings.compressClientRequest(true)
    val a : Array[Byte] = data.readAllBytes()
    val is : InputStream = new ByteArrayInputStream("".getBytes())
    Try(clientV2.insert(table, new ByteArrayInputStream(a),  ClickHouseFormat.valueOf(inputFormat), insertSettings).get()) match {
      case Success(resp : InsertResponse) => Right(deserializer(is))
      case Failure(se: ServerException) =>
        Left(CHServerException(se.getCode, se.getMessage, Some(nodeSpec), Some(se)))
      case Failure(ex) => Left(CHClientException(ex.getMessage, Some(nodeSpec), Some(ex)))
    }
  }

  def syncQuery[OUT](
    sql: String,
    outputFormat: String,
    deserializer: InputStream => SimpleOutput[OUT],
    settings: Map[String, String]
  ): Either[CHException, SimpleOutput[OUT]] = {
    val queryId = nextQueryId()
    onExecuteQuery(queryId, sql)
    val querySettings: QuerySettings = new QuerySettings()
    val clickHouseFormat = ClickHouseFormat.valueOf(outputFormat)
    querySettings.setFormat(clickHouseFormat)
    querySettings.setQueryId(queryId)
    settings.foreach { case (k, v) => querySettings.setOption(k, v) }
    Try(clientV2.query(sql, querySettings).get(timeout,  TimeUnit.MILLISECONDS)) match {
      case Success(response: QueryResponse) => Right(deserializer(response.getInputStream))
      case Failure(se: ServerException) => Left(CHServerException(se.getCode, se.getMessage, Some(nodeSpec), Some(se)))
      case Failure(ex: Exception) => Left(CHClientException(ex.getMessage, Some(nodeSpec), Some(ex)))
    }
  }

  def syncQueryAndCheck[OUT](
    sql: String,
    outputFormat: String,
    deserializer: InputStream => SimpleOutput[OUT],
    settings: Map[String, String]
  ): SimpleOutput[OUT] = syncQuery[OUT](sql, outputFormat, deserializer, settings) match {
    case Left(rethrow) => throw rethrow
    case Right(output) => output
  }

  // //////////////////////////////////////////////////////////////////////////////
  // ///////////////////////// ret QueryResponse /////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def queryAndCheck(
    sql: String,
    outputFormat: String,
    settings: Map[String, String] = Map.empty
  ): QueryResponse = {
    val queryId = nextQueryId()
    onExecuteQuery(queryId, sql)

    val querySettings : QuerySettings = new QuerySettings()
    val clickHouseFormat = ClickHouseFormat.valueOf(outputFormat)
    querySettings.setFormat(clickHouseFormat)
    querySettings.setQueryId(queryId)
    settings.foreach { case (k, v) => querySettings.setOption(k , v) }

    Try(clientV2.query(sql, querySettings).get(timeout,  TimeUnit.MILLISECONDS)) match {
      case Success(response: QueryResponse) => response
      case Failure( se : ServerException) => throw CHServerException(se.getCode, se.getMessage, Some(nodeSpec), Some(se))
      case Failure( ex : Exception) => throw CHClientException(ex.getMessage, Some(nodeSpec), Some(ex))
    }
  }

  // //////////////////////////////////////////////////////////////////////////////
  // /////////////////////////////////// Hook /////////////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def onExecuteQuery(queryId: String, sql: String): Unit = log.debug(
    s"""Execute ClickHouse SQL [$queryId]:
       |$sql
       |""".stripMargin
  )
  def ping(timeout: Int = timeout) =
    clientV2.ping(timeout)
}
