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

import com.clickhouse.spark.Logging
import com.clickhouse.client._
import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.data.{ClickHouseCompression, ClickHouseFormat}
import com.clickhouse.spark.exception.{CHClientException, CHException, CHServerException}
import com.clickhouse.spark.format.{
  JSONCompactEachRowWithNamesAndTypesSimpleOutput,
  JSONEachRowSimpleOutput,
  NamesAndTypes,
  SimpleOutput
}
import com.clickhouse.spark.Utils.RuntimeDetector.detectRuntime
import com.clickhouse.spark.spec.NodeSpec
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

import java.io.InputStream
import java.util.UUID
import scala.util.{Failure, Success, Try}

object NodeClient {
  def apply(node: NodeSpec): NodeClient = new NodeClient(node)
}

class NodeClient(val nodeSpec: NodeSpec) extends AutoCloseable with Logging {
  // TODO: add configurable timeout
  private val timeout: Int = 30000

  private lazy val userAgent: String = {
    val title = getClass.getPackage.getImplementationTitle
    val version = getClass.getPackage.getImplementationVersion
    buildUserAgent(title, version)
  }

  private def buildUserAgent(title: String, version: String): String =
    (Option(title), Option(version)) match {
      case (Some(t), Some(v)) =>
        parseVersionString(v) match {
          case Some((spark, scala, connector)) =>
            val runtimeSuffix = getRuntimeEnvironmentSuffix()
            s"$t/$connector (fv:spark/$spark, lv:scala/$scala$runtimeSuffix)"
          case None => "Spark-ClickHouse-Connector"
        }
      case _ => "Spark-ClickHouse-Connector"
    }

  private def parseVersionString(version: String): Option[(String, String, String)] =
    version.split("_") match {
      case Array(spark, scala, connector, _*) => Some((spark, scala, connector))
      case _ => None
    }

  private def getRuntimeEnvironmentSuffix(): String =
    if (shouldInferRuntime()) {
      detectRuntime()
        .filter(_.nonEmpty)
        .fold("")(env => s", env:$env")
    } else {
      ""
    }

  private def shouldInferRuntime(): Boolean =
    nodeSpec.infer_runtime_env.equalsIgnoreCase("true") || nodeSpec.infer_runtime_env == "1"

  private val node: ClickHouseNode = ClickHouseNode.builder()
    .options(nodeSpec.options)
    .host(nodeSpec.host)
    .port(nodeSpec.protocol, nodeSpec.port)
    .database(nodeSpec.database)
    .credentials(ClickHouseCredentials.fromUserAndPassword(nodeSpec.username, nodeSpec.password))
    .build()

  private val client: ClickHouseClient = ClickHouseClient.builder()
    .option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinary)
    .option(
      ClickHouseClientOption.PRODUCT_NAME,
      userAgent
    )
    .nodeSelector(ClickHouseNodeSelector.of(node.getProtocol))
    .build()

  override def close(): Unit = client.close()

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
    inputCompressionType: ClickHouseCompression = ClickHouseCompression.NONE,
    data: InputStream,
    settings: Map[String, String] = Map.empty
  ): Either[CHException, SimpleOutput[ObjectNode]] =
    syncInsert(
      database,
      table,
      inputFormat,
      inputCompressionType,
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
    inputCompressionType: ClickHouseCompression,
    data: InputStream,
    outputFormat: String,
    deserializer: InputStream => SimpleOutput[OUT],
    settings: Map[String, String]
  ): Either[CHException, SimpleOutput[OUT]] = {
    val queryId = nextQueryId()
    val sql = s"INSERT INTO `$database`.`$table` FORMAT $inputFormat"
    onExecuteQuery(queryId, sql)
    val req = client.write(node)
      .query(sql, queryId)
      .decompressClientRequest(inputCompressionType)
      .format(ClickHouseFormat.valueOf(outputFormat))
    settings.foreach { case (k, v) => req.set(k, v) }
    Try(req.data(data).executeAndWait()) match {
      case Success(resp) => Right(deserializer(resp.getInputStream))
      case Failure(ex: ClickHouseException) =>
        Left(CHServerException(ex.getErrorCode, ex.getMessage, Some(nodeSpec), Some(ex)))
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
    val req = client.read(node)
      .query(sql, queryId).asInstanceOf[ClickHouseRequest[_]]
      .format(ClickHouseFormat.valueOf(outputFormat)).asInstanceOf[ClickHouseRequest[_]]
      .option(ClickHouseClientOption.CONNECTION_TIMEOUT, timeout).asInstanceOf[ClickHouseRequest[_]]
    settings.foreach { case (k, v) => req.set(k, v).asInstanceOf[ClickHouseRequest[_]] }
    Try(req.executeAndWait()) match {
      case Success(resp) => Right(deserializer(resp.getInputStream))
      case Failure(ex: ClickHouseException) =>
        Left(CHServerException(ex.getErrorCode, ex.getMessage, Some(nodeSpec), Some(ex)))
      case Failure(ex) => Left(CHClientException(ex.getMessage, Some(nodeSpec), Some(ex)))
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
  // ///////////////////////// ret ClickHouseResponse /////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def queryAndCheck(
    sql: String,
    outputFormat: String,
    outputCompressionType: ClickHouseCompression,
    settings: Map[String, String] = Map.empty
  ): ClickHouseResponse = {
    val queryId = nextQueryId()
    onExecuteQuery(queryId, sql)
    val req = client.read(node)
      .query(sql, queryId).asInstanceOf[ClickHouseRequest[_]]
      .compressServerResponse(outputCompressionType).asInstanceOf[ClickHouseRequest[_]]
      .format(ClickHouseFormat.valueOf(outputFormat)).asInstanceOf[ClickHouseRequest[_]]
      .option(ClickHouseClientOption.CONNECTION_TIMEOUT, timeout).asInstanceOf[ClickHouseRequest[_]]
    settings.foreach { case (k, v) => req.set(k, v).asInstanceOf[ClickHouseRequest[_]] }
    Try(req.executeAndWait()) match {
      case Success(resp) => resp
      case Failure(ex: ClickHouseException) =>
        throw CHServerException(ex.getErrorCode, ex.getMessage, Some(nodeSpec), Some(ex))
      case Failure(ex) => throw CHClientException(ex.getMessage, Some(nodeSpec), Some(ex))
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
    client.ping(node, timeout)
}
