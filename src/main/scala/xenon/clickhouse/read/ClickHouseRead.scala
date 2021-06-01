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

import java.time.ZoneId

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class ClickHouseScanBuilder(
  jobDesc: ScanJobDesc,
  // some thoughts on schema.
  // we can divide the columns into 2 kinks:
  // 1. materialized columns, which store the values on local/remote storage
  // 2. calculated columns, which calculate the values on-the-fly when reading
  // but consider that usually CPU would not be bottleneck of clickhouse but IO does, it's not properly suppose that
  // reading calculated columns is expensive than materialized columns
  physicalSchema: StructType,
  metadataSchema: StructType,
  partitionTransforms: Array[Transform]
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  implicit private val tz: ZoneId = jobDesc.tz

  // default read not include meta columns, like _shard_num UInt32 of Distributed tables.
  private var readSchema: StructType = physicalSchema.copy()

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {

    Array()
  }

  override def pushedFilters(): Array[Filter] = Array()

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    // what if column required buf not exists?
    this.readSchema = StructType(physicalSchema.filter(field => requiredCols.contains(field.name)))
  }

  override def build(): Scan =
    new ClickHouseBatchScan(jobDesc.copy(readSchema = readSchema))
}

class ClickHouseBatchScan(jobDesc: ScanJobDesc)
    extends Scan with Batch with PartitionReaderFactory {

  // may contains meta columns
  override def readSchema(): StructType = jobDesc.readSchema

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory = this

  override def planInputPartitions(): Array[InputPartition] = Array(ClickHouseWholeTable)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition.asInstanceOf[ClickHouseInputPartition] match {
      case ClickHouseInputPartition(None, None) =>
        new ClickHouseReader(jobDesc)
      case ClickHouseInputPartition(Some(shard), Some(partition)) => ???
      case _ => ???
    }
}
