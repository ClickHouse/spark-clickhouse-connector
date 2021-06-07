package xenon.clickhouse.grpc

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.spark.sql.clickhouse.ClickHouseAnalysisException
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.spec.NodeSpec
import xenon.clickhouse.Logging
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

  def syncQuery(sql: String): Either[GRPCException, Result] = {
    onExecuteQuery(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setOutputFormat("JSON")
      .build
    executeQuery(queryInfo)
  }

  def syncQueryAndCheck(sql: String): Result = syncQuery(sql) match {
    case Left(exception) => throw new ClickHouseAnalysisException(exception)
    case Right(result) => result
  }

  def syncQueryWithStreamOutput(sql: String): Iterator[Result] = {
    onExecuteQuery(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setOutputFormat("JSONCompactEachRowWithNamesAndTypes")
      .build
    blockingStub.executeQueryWithStreamOutput(queryInfo).asScala
  }

  def syncInsert(
    database: String,
    table: String,
    inputFormat: String,
    data: ByteString
  ): Either[GRPCException, Result] = {
    val sql = s"INSERT INTO `$database`.`$table` FORMAT $inputFormat"
    onExecuteQuery(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setInputDataBytes(data)
      .setOutputFormat("JSON")
      .build
    executeQuery(queryInfo)
  }

  private def executeQuery(request: QueryInfo): Either[GRPCException, Result] =
    blockingStub.executeQuery(request) match {
      case result: Result if result.getException.getCode == OK.code => Right(result)
      case result: Result => Left(result.getException)
    }

  protected def onExecuteQuery(sql: String): Unit = log.debug("Execute ClickHouse SQL:\n{}", sql)

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
