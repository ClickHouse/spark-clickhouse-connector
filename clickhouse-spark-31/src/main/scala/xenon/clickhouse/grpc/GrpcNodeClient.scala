package xenon.clickhouse.grpc

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import xenon.clickhouse.exception.ClickHouseErrCode._

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.spark.sql.clickhouse.ClickHouseAnalysisException
import xenon.clickhouse.spec.NodeSpec
import xenon.clickhouse.Logging
import xenon.protocol.grpc.{ClickHouseGrpc, QueryInfo, Result, Exception => GException}

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
  lazy val futureStub: ClickHouseGrpc.ClickHouseFutureStub = ClickHouseGrpc.newFutureStub(channel)

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
          log.error("error on shutdown grpc channel", exception)
          channel.shutdownNow()
      }
    }
  }

  def shutdownNow(): Unit = synchronized {
    if (!channel.isShutdown) {
      channel.shutdownNow()
      try channel.awaitTermination(10, TimeUnit.SECONDS)
      catch {
        case NonFatal(exception) =>
          log.error("error on shutdown grpc channel", exception)
          channel.shutdownNow()
      }
    }
  }

  def syncQuery(sql: String): Either[GException, Result] = {
    onExecutingSQL(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setOutputFormat("JSON")
      .build
    executeQuery(queryInfo)
  }

  def syncQueryWithStreamOutput(sql: String): Iterator[Result] = {
    onExecutingSQL(sql)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setOutputFormat("JSONCompactEachRowWithNamesAndTypes")
      .build
    blockingStub.executeQueryWithStreamOutput(queryInfo).asScala
  }

  def syncQueryAndCheck(sql: String): Result = syncQuery(sql) match {
    case Left(exception) => throw new ClickHouseAnalysisException(exception)
    case Right(result) => result
  }

  def syncInsert(
    database: String,
    table: String,
    inputFormat: String,
    data: ByteString
  ): Either[GException, Result] = {
    val sql = s"INSERT INTO `$database`.`$table` FORMAT $inputFormat"
    onExecutingSQL(sql)
    //val dataBytes = data.map(ByteString.copyFrom).reduce((l, r) => l concat r)
    val queryInfo = QueryInfo.newBuilder(baseQueryInfo)
      .setQuery(sql)
      .setQueryId(UUID.randomUUID.toString)
      .setInputDataBytes(data)
      .setOutputFormat("JSON")
      .build
    executeQuery(queryInfo)
  }

  def executeQuery(request: QueryInfo): Either[GException, Result] =
    blockingStub.executeQuery(request) match {
      case result: Result if result.getException.getCode == OK.code => Right(result)
      case result: Result => Left(result.getException)
    }

  protected def onExecutingSQL(sql: String): Unit = {
    log.debug("Execute ClickHouse SQL:\n{}", sql)
  }
}
