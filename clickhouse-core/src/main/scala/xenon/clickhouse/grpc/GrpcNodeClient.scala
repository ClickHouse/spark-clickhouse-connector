package xenon.clickhouse.grpc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import xenon.clickhouse.Utils.om
import xenon.clickhouse.exception.ClickHouseServerException
import xenon.clickhouse.format.{JSONCompactEachRowWithNamesAndTypesStreamOutput, JSONEachRowSimpleOutput, SimpleOutput, StreamOutput}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import xenon.clickhouse.Logging
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.spec.NodeSpec
import xenon.protocol.grpc.{ClickHouseGrpc, QueryInfo, Result, Exception => GRPCException}

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
    blockingStub.executeQuery(request) match {
      case result: Result if result.getException.getCode == OK.code =>
        val records = result.getOutput
          .lines
          .filter(_.nonEmpty)
          .map(line => om.readValue[ObjectNode](line))
          .toSeq
        Right(new JSONEachRowSimpleOutput(records))
      case result: Result => Left(result.getException)
    }

  protected def onExecuteQuery(sql: String): Unit = log.debug("Execute ClickHouse SQL:\n{}", sql)

  def onReceiveResult(result: Result): Unit = {
    if (result.getException.getCode != OK.code)
      throw new ClickHouseServerException(result.getException)
  }

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
}
