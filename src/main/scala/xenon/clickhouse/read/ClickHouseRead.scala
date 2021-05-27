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

package xenon.clickhouse.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec, ShardSpec}

import java.time.ZoneId

class ClickHouseScanBuilder(
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  // some thoughts on schema.
  // we can divide the columns into 2 kinks:
  // 1. materialized columns, which store the values on local/remote storage
  // 2. calculated columns, which calculate the values on-the-fly when reading
  // but consider that usually CPU would not be bottleneck of clickhouse but IO does, it's not properly suppose that
  // reading calculated columns is expensive than materialized columns
  physicalSchema: StructType,
  distWriteUseClusterNodes: Boolean,
  distWriteConvertToLocal: Boolean
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var readSchema: StructType = physicalSchema.copy()

  override def build(): Scan =
    new ClickHouseBatchScan(node, cluster, tz, database, table, readSchema)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = Array()

  override def pushedFilters(): Array[Filter] = Array()

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    // what if column required buf not exists?
    this.readSchema = StructType(physicalSchema.filter(field => requiredCols.contains(field.name)))
  }
}

case class ClickHouseInputPartition(
  shard: Option[ShardSpec],
  partition: Option[String]
) extends InputPartition {

  override def preferredLocations(): Array[String] = shard match {
    case Some(ShardSpec(_, _, replicas)) => replicas.map(_.node.host).toArray
    case None => Array()
  }
}

object ClickHouseWholeTable extends ClickHouseInputPartition(None, None)

class ClickHouseBatchScan(
  node: NodeSpec,
  cluster: Option[ClusterSpec] = None,
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  override val readSchema: StructType,
  filterExpr: String = "1=1"
) extends Scan
    with Batch {

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = Array(ClickHouseWholeTable)

  override def createReaderFactory(): PartitionReaderFactory =
    new ClickHouseReaderFactory(node, cluster, tz, database, table, readSchema, filterExpr)
}

class ClickHouseReaderFactory(
  node: NodeSpec,
  cluster: Option[ClusterSpec] = None,
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  readSchema: StructType,
  filterExpr: String
) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition.asInstanceOf[ClickHouseInputPartition] match {
      case ClickHouseInputPartition(None, None) =>
        new ClickHouseReader(node, cluster, tz, database, table, readSchema, filterExpr)
      case ClickHouseInputPartition(Some(shard), Some(partition)) => ???
      case _ => ???
    }
}
