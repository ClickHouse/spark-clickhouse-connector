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

package xenon.clickhouse.grpc

import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import xenon.clickhouse.{Logging, Utils}
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.exception.ClickHouseServerException
import xenon.clickhouse.format._
import xenon.clickhouse.spec.NodeSpec
import xenon.protocol.grpc.{ClickHouseGrpc, Exception => GRPCException, LogEntry, QueryInfo, Result}
import xenon.protocol.grpc.LogsLevel._

object GrpcNodeClient {
  def apply(node: NodeSpec): GrpcNodeClient = new GrpcNodeClient(node)
}

class GrpcNodeClient(val node: NodeSpec) extends AutoCloseable with Logging {

  private lazy val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(node.host, node.grpc_port.get)
    .usePlaintext
    .asInstanceOf[ManagedChannelBuilder[_]]
    .build

  lazy val blockingStub: ClickHouseGrpc.ClickHouseBlockingStub =
    ClickHouseGrpc
      .newBlockingStub(channel)
      .withMaxInboundMessageSize(128 * 1024 * 1024) // 128M

  private lazy val baseQueryInfo = QueryInfo.newBuilder
    .setUserName(node.username)
    .setPassword(node.password)
    .buildPartial

  override def close(): Unit = synchronized {
    if (!channel.isShutdown) {
      channel.shutdown()
      try channel.awaitTermination(10, TimeUnit.SECONDS)
      catch {
        case NonFatal(exception) =>
          log.error("Error on shutdown gRPC channel, force shutdown.", exception)
          shutdownNow()
      }
    }
  }

  private def shutdownNow(): Unit = synchronized {
    if (!channel.isShutdown) {
      channel.shutdownNow()
      try channel.awaitTermination(3, TimeUnit.SECONDS)
      catch {
        case NonFatal(exception) =>
          log.error("Error on shutdown gRPC channel, abandon.", exception)
      }
    }
  }

  private def nextQueryId(): String = UUID.randomUUID.toString

  // //////////////////////////////////////////////////////////////////////////////
  // ///////////////////////// Synchronized Normal API ////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def syncQueryOutputJSONEachRow(
    sql: String,
    settings: Map[String, String] = Map.empty
  ): Either[GRPCException, SimpleOutput[ObjectNode]] =
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
    inputCompressionType: String = "none",
    data: ByteString,
    settings: Map[String, String] = Map.empty
  ): Either[GRPCException, SimpleOutput[ObjectNode]] =
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
    inputCompressionType: String,
    data: ByteString,
    outputFormat: String,
    deserializer: ByteString => SimpleOutput[OUT],
    settings: Map[String, String]
  ): Either[GRPCException, SimpleOutput[OUT]] = {
    val queryId = nextQueryId()
    val sql = s"INSERT INTO `$database`.`$table` FORMAT $inputFormat"
    onExecuteQuery(queryId, sql)
    val builder = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(queryId)
      .setInputData(data)
      .setInputCompressionType(inputCompressionType)
      .setOutputFormat(outputFormat)
      .putAllSettings(settings.asJava)
    val queryInfo = builder.build
    executeQuery(queryInfo, deserializer)
  }

  def syncQuery[OUT](
    sql: String,
    outputFormat: String,
    deserializer: ByteString => SimpleOutput[OUT],
    settings: Map[String, String]
  ): Either[GRPCException, SimpleOutput[OUT]] = {
    val queryId = nextQueryId()
    onExecuteQuery(queryId, sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(queryId)
      .setOutputFormat(outputFormat)
      .putAllSettings(settings.asJava)
      .build
    executeQuery(queryInfo, deserializer)
  }

  def syncQueryAndCheck[OUT](
    sql: String,
    outputFormat: String,
    deserializer: ByteString => SimpleOutput[OUT],
    settings: Map[String, String]
  ): SimpleOutput[OUT] = syncQuery[OUT](sql, outputFormat, deserializer, settings) match {
    case Left(exception) => throw new ClickHouseServerException(exception, Some(node))
    case Right(output) => output
  }

  private def executeQuery[OUT](
    request: QueryInfo,
    deserializer: ByteString => SimpleOutput[OUT]
  ): Either[GRPCException, SimpleOutput[OUT]] =
    Some(blockingStub.executeQuery(request))
      .map { result => onReceiveResult(result, false); result }
      .get match {
      case result: Result if result.getException.getCode == OK.code => Right(deserializer(result.getOutput))
      case result: Result => Left(result.getException)
    }

  // //////////////////////////////////////////////////////////////////////////////
  // ///////////////////////// Synchronized Stream API ////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def syncStreamQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(
    sql: String,
    settings: Map[String, String] = Map.empty
  ): StreamOutput[Array[JsonNode]] =
    syncStreamQueryAndCheck(
      sql,
      "JSONCompactEachRowWithNamesAndTypes",
      JSONCompactEachRowWithNamesAndTypesStreamOutput.deserializeStream,
      settings
    )

  def syncStreamQueryAndCheck[OUT](
    sql: String,
    outputFormat: String,
    outputStreamDeserializer: Iterator[ByteString] => StreamOutput[OUT],
    settings: Map[String, String]
  ): StreamOutput[OUT] = {
    val queryId = nextQueryId()
    onExecuteQuery(queryId, sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(queryId)
      .setOutputFormat(outputFormat)
      .putAllSettings(settings.asJava)
      .build
    val outputIterator = blockingStub.executeQueryWithStreamOutput(queryInfo)
      .asScala
      .map { result => onReceiveResult(result, false); result }
      .map { result =>
        if (result.getException.getCode == OK.code) Right(result.getOutput) else Left(result.getException)
      }
      .map {
        case Left(gRPCException) => throw new ClickHouseServerException(gRPCException, Some(node))
        case Right(output) => output
      }
    outputStreamDeserializer(outputIterator)
  }

  // //////////////////////////////////////////////////////////////////////////////
  // /////////////////////////////////// Hook /////////////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////

  def onExecuteQuery(queryId: String, sql: String): Unit = log.debug(
    s"""Execute ClickHouse SQL [$queryId]:
       |$sql
       |""".stripMargin
  )

  def onReceiveResult(result: Result, throwException: Boolean = true): Unit = {
    result.getLogsList.asScala.foreach { le: LogEntry =>
      val logTime = Instant.ofEpochSecond(le.getTime.toLong, le.getTimeMicroseconds * 1000)
        .atZone(ZoneOffset.UTC)
        .withZoneSameInstant(ZoneId.systemDefault())
        .format(Utils.dateTimeFmt)

      val message = s"$logTime [${le.getQueryId}] ${le.getText}"
      le.getLevel match {
        case UNRECOGNIZED | LOG_NONE | LOG_FATAL | LOG_CRITICAL | LOG_ERROR => log.error(message)
        case LOG_WARNING | LOG_NOTICE => log.warn(message)
        case LOG_INFORMATION => log.info(message)
        case LOG_DEBUG => log.debug(message)
        case LOG_TRACE => log.trace(message)
      }
    }
    if (throwException && result.getException.getCode != OK.code)
      throw new ClickHouseServerException(result.getException, Some(node))
  }
}
