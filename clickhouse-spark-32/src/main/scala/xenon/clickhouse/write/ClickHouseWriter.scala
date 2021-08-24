/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.write

import java.time.Duration

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.clickhouse.JsonFormatUtils
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import xenon.clickhouse._
import xenon.clickhouse.exception.{ClickHouseClientException, ClickHouseServerException, RetryableClickHouseException}
import xenon.clickhouse.grpc.{GrpcClusterClient, GrpcNodeClient}
import xenon.clickhouse.spec.DistributedEngineSpec

class ClickHouseAppendWriter(jobDesc: WriteJobDesc)
    extends DataWriter[InternalRow] with SQLConfHelper with Logging {

  val batchSize: Int = conf.getConf(WRITE_BATCH_SIZE)
  val writeDistributedUseClusterNodes: Boolean = conf.getConf(WRITE_DISTRIBUTED_USE_CLUSTER_NODES)
  val writeDistributedConvertLocal: Boolean = conf.getConf(WRITE_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = jobDesc.targetDatabase(writeDistributedConvertLocal)
  val table: String = jobDesc.targetTable(writeDistributedConvertLocal)

  // put the node select strategy in executor side because we need to calculate shard and don't know the records
  // util DataWriter#write(InternalRow) invoked.
  private lazy val grpcClient: Either[GrpcClusterClient, GrpcNodeClient] =
    (jobDesc.tableEngineSpec, writeDistributedUseClusterNodes, writeDistributedConvertLocal) match {
      case (_: DistributedEngineSpec, _, true) =>
        // FIXME: Since we don't know the corresponding ClickHouse shard and partition of the RDD partition now,
        //        we can't pick the right nodes from cluster here
        throw ClickHouseClientException(s"${WRITE_DISTRIBUTED_CONVERT_LOCAL.key} is not support yet.")
      case (_: DistributedEngineSpec, true, _) =>
        val clusterSpec = jobDesc.cluster.get
        log.info(s"Connect to ClickHouse cluster ${clusterSpec.name}, which has ${clusterSpec.nodes.length} nodes.")
        Left(GrpcClusterClient(clusterSpec))
      case _ =>
        val nodeSpec = jobDesc.node
        log.info("Connect to ClickHouse single node.")
        Right(GrpcNodeClient(nodeSpec))
    }

  def grpcNodeClient: GrpcNodeClient = grpcClient match {
    case Left(clusterClient) => clusterClient.node()
    case Right(nodeClient) => nodeClient
  }

  val buf: ArrayBuffer[ByteString] = new ArrayBuffer[ByteString](batchSize)

  override def write(record: InternalRow): Unit = {
    // TODO evaluate shard, flush if shard change or reach batchSize
    buf += JsonFormatUtils.row2Json(record, jobDesc.dataSetSchema, jobDesc.tz)
    if (buf.size == batchSize) flush()
  }

  override def commit(): WriterCommitMessage = {
    if (buf.nonEmpty) flush()
    CommitMessage(s"Job[${jobDesc.queryId}]: commit")
  }

  override def abort(): Unit = close()

  override def close(): Unit = grpcClient match {
    case Left(clusterClient) => clusterClient.close()
    case Right(nodeClient) => nodeClient.close()
  }

  def flush(): Unit =
    Utils.retry[Unit, RetryableClickHouseException](
      conf.getConf(WRITE_MAX_RETRY),
      Duration.ofSeconds(conf.getConf(WRITE_RETRY_INTERVAL))
    ) {
      grpcNodeClient.syncInsert(database, table, "JSONEachRow", buf.reduce((l, r) => l concat r)) match {
        case Right(_) => buf.clear
        case Left(retryable) if conf.getConf(WRITE_RETRYABLE_ERROR_CODES).contains(retryable.getCode) =>
          throw new RetryableClickHouseException(retryable)
        case Left(rethrow) => throw new ClickHouseServerException(rethrow)
      }
    } match {
      case Success(_) => log.info(s"Job[${jobDesc.queryId}]: flush batch")
      case Failure(rethrow) => throw rethrow
    }
}

class ClickHouseTruncateWriter(jobDesc: WriteJobDesc)
    extends DataWriter[InternalRow] with SQLConfHelper with Logging {

  val truncateDistributedConvertToLocal: Boolean = conf.getConf(TRUNCATE_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = jobDesc.targetDatabase(truncateDistributedConvertToLocal)
  val table: String = jobDesc.targetTable(truncateDistributedConvertToLocal)

  lazy val grpcNodeClient: GrpcNodeClient = GrpcNodeClient(jobDesc.node)

  def clusterExpr: String = if (truncateDistributedConvertToLocal) s"ON CLUSTER ${jobDesc.cluster.get.name}" else ""

  override def write(record: InternalRow): Unit = {}

  override def commit(): WriterCommitMessage = {
    grpcNodeClient.syncQueryAndCheck(s"TRUNCATE TABLE `$database`.`$table` $clusterExpr")
    CommitMessage(s"Job[${jobDesc.queryId}]: commit truncate")
  }

  override def abort(): Unit = grpcNodeClient.close()

  override def close(): Unit = grpcNodeClient.close()
}
