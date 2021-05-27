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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec}
import xenon.clickhouse.write.WriteAction._

import java.time.ZoneId

class ClickHouseWriteBuilder(
  queryId: String,
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  schema: StructType,
  distWriteUseClusterNodes: Boolean,
  distWriteConvertToLocal: Boolean
) extends WriteBuilder
    with SupportsTruncate {

  private var action: WriteAction = APPEND

  override def buildForBatch(): ClickHouseBatchWrite =
    new ClickHouseBatchWrite(queryId, node, cluster, tz, database, table, schema, action)

  override def truncate(): WriteBuilder = {
    this.action = TRUNCATE
    this
  }
}

class ClickHouseBatchWrite(
  queryId: String,
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  schema: StructType,
  action: WriteAction
) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new ClickHouseDataWriterFactory(queryId, node, cluster, tz, database, table, schema, action)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class ClickHouseDataWriterFactory(
  queryId: String,
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  schema: StructType,
  action: WriteAction
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = action match {
    case APPEND => new ClickHouseBatchWriter(queryId, node, cluster, tz, database, table, schema)
    case TRUNCATE => new ClickHouseTruncateWriter(queryId, node, cluster, database, table)
  }
}
