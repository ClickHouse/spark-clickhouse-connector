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

import org.apache.spark.sql.JsonFormatUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import xenon.clickhouse._
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.exception.RetryableClickHouseException
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec}
import java.time.{Duration, ZoneId}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

abstract class ClickHouseWriter extends DataWriter[InternalRow] with Logging {

  val queryId: String
  val node: NodeSpec
  val cluster: Option[ClusterSpec]
  val database: String
  val table: String
  val distWriteUseClusterNodes: Boolean
  val distWriteConvertToLocal: Boolean

  @transient lazy val _grpcConn: GrpcNodeClient = GrpcNodeClient(node)

  def grpcNodeClient: GrpcNodeClient = _grpcConn

  override def abort(): Unit = grpcNodeClient.shutdownNow()

  override def close(): Unit = grpcNodeClient.close()
}

class ClickHouseBatchWriter(
  val queryId: String,
  val node: NodeSpec,
  val cluster: Option[ClusterSpec],
  tz: Either[ZoneId, ZoneId],
  val database: String,
  val table: String,
  val schema: StructType,
  val batchSize: Int,
  val distWriteUseClusterNodes: Boolean,
  val distWriteConvertToLocal: Boolean
) extends ClickHouseWriter {

  val ckSchema: Map[String, String] = SchemaUtil
    .toClickHouseSchema(schema)
    .toMap

  val buf: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]](batchSize)

  override def write(record: InternalRow): Unit = {
    buf += JsonFormatUtil.row2Json(record, schema, tz.merge)
    if (buf.size == batchSize) flush()
  }

  override def commit(): WriterCommitMessage = {
    if (buf.nonEmpty) flush()
    CommitMessage("flush write")
  }

  def flush(): Unit =
    Utils.retry[Unit, RetryableClickHouseException](3, Duration.ofSeconds(10)) {
      grpcNodeClient.syncInsert(database, table, "JSONEachRow", buf.toArray) match {
        case Left(exception) if exception.getCode == MEMORY_LIMIT_EXCEEDED.code =>
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
  val table: String,
  val distWriteUseClusterNodes: Boolean,
  val distWriteConvertToLocal: Boolean
) extends ClickHouseWriter {

  private lazy val clusterExpr = cluster match {
    case Some(cluster) => s"ON CLUSTER ${cluster.name}"
    case None => ""
  }

  override def write(record: InternalRow): Unit = {
    // TODO check, forbid truncate Distribute table
  }

  override def commit(): WriterCommitMessage = {
    grpcNodeClient.syncQueryAndCheck(s"TRUNCATE TABLE `$database`.`$table` $clusterExpr")
    CommitMessage("commit truncate")
  }
}
