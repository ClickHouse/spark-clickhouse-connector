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

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.{ClickHouseAnalysisException, ClickHouseSQLConf}
import org.apache.spark.sql.clickhouse.util.JsonFormatUtil
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import xenon.clickhouse._
import xenon.clickhouse.exception.RetryableClickHouseException
import xenon.clickhouse.spec.DistributedEngineSpec

class ClickHouseAppendWriter(jobDesc: WriteJobDesc) extends DataWriter[InternalRow] with SQLConfHelper with Logging {

  val batchSize: Int = conf.getConf(ClickHouseSQLConf.WRITE_BATCH_SIZE)
  val writeDistributedUseClusterNodes: Boolean = conf.getConf(ClickHouseSQLConf.WRITE_DISTRIBUTED_USE_CLUSTER_NODES)
  val writeDistributedConvertLocal: Boolean = conf.getConf(ClickHouseSQLConf.WRITE_DISTRIBUTED_CONVERT_LOCAL)

  // DataSet schema, not ClickHouse table schema
  val ckSchema: Map[String, String] = SchemaUtil.toClickHouseSchema(jobDesc.schema).toMap

  val database: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if writeDistributedConvertLocal => dist.local_db
    case _ => jobDesc.tableSpec.database
  }

  val table: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if writeDistributedConvertLocal => dist.local_table
    case _ => jobDesc.tableSpec.name
  }

  private lazy val grpcClient: Either[GrpcClusterClient, GrpcNodeClient] =
    (jobDesc.tableEngineSpec, writeDistributedUseClusterNodes, writeDistributedConvertLocal) match {
      case (_: DistributedEngineSpec, _, true) =>
        // FIXME: Since we don't know the corresponding ClickHouse shard and partition of the RDD partition now,
        //        we can't pick the right nodes from cluster here
        throw ClickHouseAnalysisException(
          s"${ClickHouseSQLConf.WRITE_DISTRIBUTED_CONVERT_LOCAL.key} is not support yet."
        )
      case (_: DistributedEngineSpec, true, _) =>
        val clusterSpec = jobDesc.cluster.get
        log.info(s"Connect to ClickHouse cluster ${clusterSpec.name}, which has ${clusterSpec.nodes.size} nodes.")
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

  val buf: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]](batchSize)

  override def write(record: InternalRow): Unit = {
    // TODO evaluate shard, flush if shard change or reach batchSize
    buf += JsonFormatUtil.row2Json(record, jobDesc.schema, jobDesc.tz)
    if (buf.size == batchSize) flush()
  }

  override def commit(): WriterCommitMessage = {
    if (buf.nonEmpty) flush()
    CommitMessage(s"Job[${jobDesc.id}]: commit")
  }

  override def abort(): Unit = close()

  override def close(): Unit = grpcClient match {
    case Left(clusterClient) => clusterClient.close()
    case Right(nodeClient) => nodeClient.close()
  }

  def flush(): Unit =
    Utils.retry[Unit, RetryableClickHouseException](
      conf.getConf(ClickHouseSQLConf.WRITE_MAX_RETRY),
      Duration.ofSeconds(conf.getConf(ClickHouseSQLConf.WRITE_RETRY_INTERVAL))
    ) {
      grpcNodeClient.syncInsert(database, table, "JSONEachRow", buf.toArray) match {
        case Left(exception)
            if conf.getConf(ClickHouseSQLConf.WRITE_RETRYABLE_ERROR_CODES).contains(exception.getCode) =>
          throw new RetryableClickHouseException(exception)
        case Right(_) => buf.clear
      }
    } match {
      case Success(_) => log.info(s"Job[${jobDesc.id}]: flush batch")
      case Failure(rethrow) => throw rethrow
    }
}

class ClickHouseTruncateWriter(
  jobDesc: WriteJobDesc
) extends DataWriter[InternalRow] with SQLConfHelper with Logging {

  val truncateDistributedConvertToLocal: Boolean = conf.getConf(ClickHouseSQLConf.TRUNCATE_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if truncateDistributedConvertToLocal => dist.local_db
    case _ => jobDesc.tableSpec.database
  }

  val table: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if truncateDistributedConvertToLocal => dist.local_table
    case _ => jobDesc.tableSpec.name
  }

  lazy val grpcNodeClient: GrpcNodeClient = GrpcNodeClient(jobDesc.node)

  def clusterExpr: String = if (truncateDistributedConvertToLocal) s"ON CLUSTER ${jobDesc.cluster.get.name}" else ""

  override def write(record: InternalRow): Unit = {}

  override def commit(): WriterCommitMessage = {
    grpcNodeClient.syncQueryAndCheck(s"TRUNCATE TABLE `$database`.`$table` $clusterExpr")
    CommitMessage(s"Job[${jobDesc.id}]: commit truncate")
  }

  override def abort(): Unit = grpcNodeClient.shutdownNow()

  override def close(): Unit = grpcNodeClient.close()
}
