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

package xenon.clickhouse.client

import java.io.InputStream
import java.util.UUID

import scala.util.{Failure, Success, Try}

import com.clickhouse.client._
import com.clickhouse.client.config.ClickHouseClientOption
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.JsonNode
import xenon.clickhouse.spec.NodeSpec
import xenon.clickhouse.Logging
import xenon.clickhouse.exception.{CHClientException, CHException, CHServerException}
import xenon.clickhouse.format._

object NodeClient {
  def apply(node: NodeSpec): NodeClient = new NodeClient(node)
}

class NodeClient(val nodeSpec: NodeSpec) extends AutoCloseable with Logging {

  private val node: ClickHouseNode = ClickHouseNode.builder()
    .host(nodeSpec.host)
    .port(nodeSpec.protocol, nodeSpec.port)
    .database(nodeSpec.database)
    .credentials(ClickHouseCredentials.fromUserAndPassword(nodeSpec.username, nodeSpec.password))
    .build()

  private val client: ClickHouseClient = ClickHouseClient.builder()
    .option(ClickHouseClientOption.ASYNC, false)
    .option(ClickHouseClientOption.FORMAT, ClickHouseFormat.RowBinary)
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
    val req = client.connect(node).write()
      .query(sql, queryId)
      .decompressClientRequest(inputCompressionType)
      .format(ClickHouseFormat.valueOf(outputFormat))
    settings.foreach { case (k, v) => req.set(k, v) }
    Try(req.data(data).executeAndWait()) match {
      case Success(resp) => Right(deserializer(resp.getInputStream))
      case Failure(ex: ClickHouseException) => Left(CHServerException(ex.getErrorCode, ex.getMessage, Some(nodeSpec)))
      case Failure(ex) => Left(CHClientException(ex.getMessage, Some(nodeSpec)))
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
    val req = client.connect(node)
      .query(sql, queryId).asInstanceOf[ClickHouseRequest[_]]
      .format(ClickHouseFormat.valueOf(outputFormat)).asInstanceOf[ClickHouseRequest[_]]
    settings.foreach { case (k, v) => req.set(k, v).asInstanceOf[ClickHouseRequest[_]] }
    Try(req.executeAndWait()) match {
      case Success(resp) => Right(deserializer(resp.getInputStream))
      case Failure(ex: ClickHouseException) => Left(CHServerException(ex.getErrorCode, ex.getMessage, Some(nodeSpec)))
      case Failure(ex) => Left(CHClientException(ex.getMessage, Some(nodeSpec)))
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
  // ///////////////////////// Synchronized Stream API ////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def syncStreamQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(
    sql: String,
    outputCompressionType: ClickHouseCompression = ClickHouseCompression.NONE,
    settings: Map[String, String] = Map.empty
  ): StreamOutput[Array[JsonNode]] =
    syncStreamQueryAndCheck(
      sql,
      "JSONCompactEachRowWithNamesAndTypes",
      outputCompressionType,
      JSONCompactEachRowWithNamesAndTypesStreamOutput.deserializeStream,
      settings
    )

  def syncStreamQueryAndCheck[OUT](
    sql: String,
    outputFormat: String,
    outputCompressionType: ClickHouseCompression,
    outputStreamDeserializer: Iterator[InputStream] => StreamOutput[OUT],
    settings: Map[String, String]
  ): StreamOutput[OUT] = {
    val queryId = nextQueryId()
    onExecuteQuery(queryId, sql)

    val req = client.connect(node)
      .query(sql, queryId).asInstanceOf[ClickHouseRequest[_]]
      .compressServerResponse(outputCompressionType).asInstanceOf[ClickHouseRequest[_]]
      .format(ClickHouseFormat.valueOf(outputFormat)).asInstanceOf[ClickHouseRequest[_]]
    settings.foreach { case (k, v) => req.set(k, v).asInstanceOf[ClickHouseRequest[_]] }
    Try(req.executeAndWait()) match {
      case Success(resp) => outputStreamDeserializer(Seq(resp.getInputStream).iterator)
      case Failure(ex: ClickHouseException) => throw CHServerException(ex.getErrorCode, ex.getMessage, Some(nodeSpec))
      case Failure(ex) => throw CHClientException(ex.getMessage, Some(nodeSpec))
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
}
