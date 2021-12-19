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

package xenon.clickhouse.write

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, SafeProjection}
import org.apache.spark.sql.clickhouse.{ExprUtils, JsonFormatUtils}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}
import xenon.clickhouse._
import xenon.clickhouse.exception._
import xenon.clickhouse.grpc.{GrpcClusterClient, GrpcNodeClient}
import xenon.clickhouse.spec.{DistributedEngineSpec, ShardUtils}

class ClickHouseAppendWriter(writeJob: WriteJobDescription)
    extends DataWriter[InternalRow] with Logging {

  val database: String = writeJob.targetDatabase(writeJob.writeOptions.convertDistributedToLocal)
  val table: String = writeJob.targetTable(writeJob.writeOptions.convertDistributedToLocal)

  private lazy val shardExpr: Option[Expression] = writeJob.sparkShardExpr match {
    case None if writeJob.tableEngineSpec.is_distributed =>
      throw ClickHouseClientException("Can not write data to a Distributed table that lacks sharding key")
    case None => None
    case Some(v2Expr) =>
      val catalystExpr = ExprUtils.toCatalyst(v2Expr, writeJob.dataSetSchema.fields)
      catalystExpr match {
        case BoundReference(_, dataType, _)
            if dataType.isInstanceOf[ByteType] // list all integral types here because we can not access `IntegralType`
              || dataType.isInstanceOf[ShortType]
              || dataType.isInstanceOf[IntegerType]
              || dataType.isInstanceOf[LongType] =>
          Some(catalystExpr)
        case BoundReference(_, dataType, _) =>
          throw ClickHouseClientException(s"Invalid data type of sharding field: $dataType")
        case unsupported: Expression =>
          log.warn(s"Unsupported expression of sharding field: $unsupported")
          None
      }
  }

  // put the node select strategy in executor side because we need to calculate shard and don't know the records
  // util DataWriter#write(InternalRow) invoked.
  private lazy val grpcClient: Either[GrpcClusterClient, GrpcNodeClient] =
    writeJob.tableEngineSpec match {
      case _: DistributedEngineSpec
          if writeJob.writeOptions.useClusterNodesForDistributed || writeJob.writeOptions.convertDistributedToLocal =>
        val clusterSpec = writeJob.cluster.get
        log.info(s"Connect to ClickHouse cluster ${clusterSpec.name}, which has ${clusterSpec.nodes.length} nodes.")
        Left(GrpcClusterClient(clusterSpec))
      case _ =>
        val nodeSpec = writeJob.node
        log.info("Connect to ClickHouse single node.")
        Right(GrpcNodeClient(nodeSpec))
    }

  def grpcNodeClient(shardNum: Option[Int]): GrpcNodeClient = grpcClient match {
    case Left(clusterClient) => clusterClient.node(shardNum)
    case Right(nodeClient) => nodeClient
  }

  var lastShardNum: Option[Int] = None
  val buf: ArrayBuffer[ByteString] = new ArrayBuffer[ByteString](writeJob.writeOptions.batchSize)

  override def write(record: InternalRow): Unit = {
    val shardNum = calcShard(record)
    if (shardNum != lastShardNum && buf.nonEmpty) flush(lastShardNum)
    lastShardNum = shardNum
    buf += JsonFormatUtils.row2Json(record, writeJob.dataSetSchema, writeJob.tz)
    if (buf.size == writeJob.writeOptions.batchSize) flush(lastShardNum)
  }

  override def commit(): WriterCommitMessage = {
    if (buf.nonEmpty) flush(lastShardNum)
    CommitMessage(s"Job[${writeJob.queryId}]: commit")
  }

  override def abort(): Unit = close()

  override def close(): Unit = grpcClient match {
    case Left(clusterClient) => clusterClient.close()
    case Right(nodeClient) => nodeClient.close()
  }

  def calcShard(record: InternalRow): Option[Int] =
    if (!writeJob.writeOptions.convertDistributedToLocal) None
    else shardExpr match {
      case Some(expr @ BoundReference(_, dataType, _)) =>
        val shardProj = SafeProjection.create(Seq(expr))
        val resultRow = shardProj(record)
        val shardValue = dataType match {
          case ByteType => resultRow.getByte(0).toLong
          case ShortType => resultRow.getShort(0).toLong
          case IntegerType => resultRow.getInt(0).toLong
          case LongType => resultRow.getLong(0)
        }
        val shardSpec = ShardUtils.calcShard(writeJob.cluster.get, shardValue)
        Some(shardSpec.num)
      case _ => None
    }

  def flush(shardNum: Option[Int]): Unit =
    Utils.retry[Unit, RetryableClickHouseException](
      writeJob.writeOptions.maxRetry,
      writeJob.writeOptions.retryInterval
    ) {
      val client = grpcNodeClient(shardNum)
      log.info(s"""Job[${writeJob.queryId}]: prepare to flush batch
                  |node: ${client.node.host}:${client.node.grpc_port}
                  |batch size: ${buf.size}
                  |cluster: ${writeJob.cluster.map(_.name)}
                  |shard: $shardNum
                  |""".stripMargin)
      client.syncInsert(
        database,
        table,
        "JSONEachRow",
        buf.reduce((l, r) => l concat r)
      ) match {
        case Right(_) => buf.clear
        case Left(retryable) if writeJob.writeOptions.retryableErrorCodes.contains(retryable.getCode) =>
          throw new RetryableClickHouseException(retryable)
        case Left(rethrow) => throw new ClickHouseServerException(rethrow)
      }
    } match {
      case Success(_) => log.info(s"Job[${writeJob.queryId}]: flush batch completed")
      case Failure(rethrow) => throw rethrow
    }
}

class ClickHouseTruncateWriter(writeJob: WriteJobDescription)
    extends DataWriter[InternalRow] with SQLConfHelper with Logging {

  val truncateDistributedConvertToLocal: Boolean = conf.getConf(TRUNCATE_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = writeJob.targetDatabase(truncateDistributedConvertToLocal)
  val table: String = writeJob.targetTable(truncateDistributedConvertToLocal)

  lazy val grpcNodeClient: GrpcNodeClient = GrpcNodeClient(writeJob.node)

  def clusterExpr: String = if (truncateDistributedConvertToLocal) s"ON CLUSTER ${writeJob.cluster.get.name}" else ""

  override def write(record: InternalRow): Unit = {}

  override def commit(): WriterCommitMessage = {
    grpcNodeClient.syncQueryAndCheck(s"TRUNCATE TABLE `$database`.`$table` $clusterExpr")
    CommitMessage(s"Job[${writeJob.queryId}]: commit truncate")
  }

  override def abort(): Unit = grpcNodeClient.close()

  override def close(): Unit = grpcNodeClient.close()
}
