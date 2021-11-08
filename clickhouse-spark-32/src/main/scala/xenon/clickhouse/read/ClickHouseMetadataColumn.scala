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

package xenon.clickhouse.read

import org.apache.spark.sql.connector.catalog.MetadataColumn
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField}

object ClickHouseMetadataColumn {
  val mergeTreeMetadataCols: Array[MetadataColumn] = Array(
    ClickHouseMetadataColumn("_part", StringType, false),
    ClickHouseMetadataColumn("_part_index", LongType, false),
    ClickHouseMetadataColumn("_part_uuid", StringType, false),
    ClickHouseMetadataColumn("_partition_id", StringType, false),
    // ClickHouseMetadataColumn("_partition_value", StringType, false),
    ClickHouseMetadataColumn("_sample_factor", DoubleType, false)
  )

  val distributeMetadataCols: Array[MetadataColumn] = Array(
    ClickHouseMetadataColumn("_table", StringType, false),
    ClickHouseMetadataColumn("_part", StringType, false),
    ClickHouseMetadataColumn("_part_index", LongType, false),
    ClickHouseMetadataColumn("_part_uuid", StringType, false),
    ClickHouseMetadataColumn("_partition_id", StringType, false),
    ClickHouseMetadataColumn("_sample_factor", DoubleType, false),
    ClickHouseMetadataColumn("_shard_num", IntegerType, false)
  )
}

case class ClickHouseMetadataColumn(
  override val name: String,
  override val dataType: DataType,
  override val isNullable: Boolean
) extends MetadataColumn {
  def toStructField: StructField = StructField(name, dataType, isNullable)
}
