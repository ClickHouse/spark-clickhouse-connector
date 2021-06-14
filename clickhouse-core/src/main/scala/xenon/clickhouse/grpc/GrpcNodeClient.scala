package xenon.clickhouse.grpc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import xenon.clickhouse.Utils.om
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.exception.ClickHouseServerException
import xenon.clickhouse.format._
import xenon.clickhouse.spec.NodeSpec
import xenon.clickhouse.{Logging, Utils}
import xenon.protocol.grpc.LogsLevel._
import xenon.protocol.grpc.{ClickHouseGrpc, LogEntry, QueryInfo, Result, Exception => GRPCException}

import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object GrpcNodeClient {
  def apply(node: NodeSpec): GrpcNodeClient = new GrpcNodeClient(node)
}

class GrpcNodeClient(node: NodeSpec) extends AutoCloseable with Logging {

  private lazy val channel: ManagedChannel = ManagedChannelBuilder
    .forAddress(node.host, node.grpc_port.get)
    .usePlaintext
    .asInstanceOf[ManagedChannelBuilder[_]]
    .build

  lazy val blockingStub: ClickHouseGrpc.ClickHouseBlockingStub = ClickHouseGrpc.newBlockingStub(channel)

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
          log.error("Error on shutdown grpc channel, force shutdown.", exception)
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
          log.error("Error on shutdown grpc channel, abandon.", exception)
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  /////////////////////////// Synchronized Normal API ////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  def syncQuery(sql: String): Either[GRPCException, SimpleOutput[ObjectNode]] = {
    onExecuteQuery(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setOutputFormat("JSONEachRow")
      .build
    executeQuery(queryInfo)
  }

  def syncQueryAndCheck(sql: String): SimpleOutput[ObjectNode] = syncQuery(sql) match {
    case Left(exception) => throw new ClickHouseServerException(exception)
    case Right(output) => output
  }

  def syncInsert(
    database: String,
    table: String,
    inputFormat: String,
    data: ByteString
  ): Either[GRPCException, SimpleOutput[ObjectNode]] = {
    val sql = s"INSERT INTO `$database`.`$table` FORMAT $inputFormat"
    onExecuteQuery(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setInputDataBytes(data)
      .setOutputFormat("JSONEachRow")
      .build
    executeQuery(queryInfo)
  }

  private def executeQuery(request: QueryInfo): Either[GRPCException, SimpleOutput[ObjectNode]] =
    Some(blockingStub.executeQuery(request))
      .map { result => onReceiveResult(result, false); result }
      .get match {
      case result: Result if result.getException.getCode == OK.code =>
        val records = result.getOutput
          .lines
          .filter(_.nonEmpty)
          .map(line => om.readValue[ObjectNode](line))
          .toSeq
        Right(new JSONEachRowSimpleOutput(records))
      case result: Result => Left(result.getException)
    }

  ////////////////////////////////////////////////////////////////////////////////
  /////////////////////////// Synchronized Stream API ////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  def syncStreamQuery(sql: String): StreamOutput[Array[JsonNode]] = {
    onExecuteQuery(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setOutputFormat("JSONCompactEachRowWithNamesAndTypes")
      .build
    val stream = blockingStub.executeQueryWithStreamOutput(queryInfo)
      .asScala
      .map { result => onReceiveResult(result); result }
      .flatMap(_.getOutput.linesIterator)
      .filter(_.nonEmpty)
      .map(line => om.readValue[Array[JsonNode]](line))

    new JSONCompactEachRowWithNamesAndTypesStreamOutput(stream)
  }

  ////////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////// Hook /////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  def onExecuteQuery(sql: String): Unit = log.debug("Execute ClickHouse SQL:\n{}", sql)

  def onReceiveResult(result: Result, throwException: Boolean = true): Unit = {
    result.getLogsList.asScala.foreach { le: LogEntry =>
      val logTime = Instant.ofEpochSecond(le.getTime.toLong, le.getTimeMicroseconds * 1000)
        .atZone(ZoneOffset.UTC)
        .withZoneSameInstant(ZoneId.systemDefault())
        .format(Utils.dateTimeFmt)

      val message = s"$logTime [${le.getQueryId}] ${le.getText}"
      le.getLevel match {
        case UNRECOGNIZED | LOG_NONE | LOG_FATAL | LOG_CRITICAL | LOG_ERROR => log.error(message)
        case LOG_WARNING => log.warn(message)
        case LOG_NOTICE | LOG_INFORMATION => log.info(message)
        case LOG_DEBUG => log.debug(message)
        case LOG_TRACE => log.trace(message)
      }
    }
    if (throwException && result.getException.getCode != OK.code)
      throw new ClickHouseServerException(result.getException)
  }
}
