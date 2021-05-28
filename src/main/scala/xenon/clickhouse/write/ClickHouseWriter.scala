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
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf
import org.apache.spark.sql.clickhouse.util.JsonFormatUtil
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import xenon.clickhouse._
import xenon.clickhouse.exception.RetryableClickHouseException
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec}

abstract class ClickHouseWriter extends DataWriter[InternalRow] with SQLConfHelper with Logging {

  def queryId: String
  def node: NodeSpec
  def cluster: Option[ClusterSpec]
  def database: String
  def table: String

  def writeDistributedUseClusterNodes: Boolean = conf.getConf(ClickHouseSQLConf.WRITE_DISTRIBUTED_USE_CLUSTER_NODES)
  def writeDistributedConvertToLocal: Boolean = conf.getConf(ClickHouseSQLConf.WRITE_DISTRIBUTED_CONVERT_LOCAL)

  @transient lazy val _grpcConn: GrpcNodeClient = GrpcNodeClient(node)

  def grpcNodeClient: GrpcNodeClient = _grpcConn

  override def abort(): Unit = grpcNodeClient.shutdownNow()

  override def close(): Unit = grpcNodeClient.close()
}

class ClickHouseAppendWriter(jobDesc: WriteJobDesc) extends ClickHouseWriter {

  override def queryId: String = jobDesc.id
  override def node: NodeSpec = jobDesc.node
  override def cluster: Option[ClusterSpec] = jobDesc.cluster
  override def database: String = jobDesc.database
  override def table: String = jobDesc.table

  val batchSize: Int = conf.getConf(ClickHouseSQLConf.WRITE_BATCH_SIZE)

  val buf: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]](batchSize)

  val ckSchema: Map[String, String] = SchemaUtil
    .toClickHouseSchema(jobDesc.schema)
    .toMap

  override def write(record: InternalRow): Unit = {
    buf += JsonFormatUtil.row2Json(record, jobDesc.schema, jobDesc.tz)
    if (buf.size == batchSize) flush()
  }

  override def commit(): WriterCommitMessage = {
    if (buf.nonEmpty) flush()
    CommitMessage("flush write")
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
      case Success(_) =>
      case Failure(rethrow) => throw rethrow
    }
}

class ClickHouseTruncateWriter(
  val queryId: String,
  val node: NodeSpec,
  val cluster: Option[ClusterSpec],
  val database: String,
  val table: String
) extends ClickHouseWriter {

  def truncateDistributedConvertToLocal: Boolean = conf.getConf(ClickHouseSQLConf.TRUNCATE_DISTRIBUTED_CONVERT_LOCAL)

  def clusterExpr: String = cluster match {
    case Some(cluster) if truncateDistributedConvertToLocal => s"ON CLUSTER ${cluster.name}"
    case _ => ""
  }

  override def write(record: InternalRow): Unit = {}

  override def commit(): WriterCommitMessage = {
    grpcNodeClient.syncQueryAndCheck(s"TRUNCATE TABLE `$database`.`$table` $clusterExpr")
    CommitMessage("commit truncate")
  }
}
