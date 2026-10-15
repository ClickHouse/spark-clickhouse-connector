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

package com.clickhouse.spark.read

import org.apache.spark.sql.connector.catalog.MetadataColumn
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField}

object ClickHouseMetadataColumn {
  val mergeTreeMetadataCols: Array[MetadataColumn] = Array(
    ClickHouseMetadataColumn("_part", StringType),
    ClickHouseMetadataColumn("_part_index", LongType),
    ClickHouseMetadataColumn("_part_uuid", StringType),
    ClickHouseMetadataColumn("_partition_id", StringType),
    // ClickHouseMetadataColumn("_partition_value", StringType),
    ClickHouseMetadataColumn("_sample_factor", DoubleType)
  )

  val distributeMetadataCols: Array[MetadataColumn] = Array(
    ClickHouseMetadataColumn("_table", StringType),
    ClickHouseMetadataColumn("_part", StringType),
    ClickHouseMetadataColumn("_part_index", LongType),
    ClickHouseMetadataColumn("_part_uuid", StringType),
    ClickHouseMetadataColumn("_partition_id", StringType),
    ClickHouseMetadataColumn("_sample_factor", DoubleType),
    ClickHouseMetadataColumn("_shard_num", IntegerType)
  )
}

case class ClickHouseMetadataColumn(
  override val name: String,
  override val dataType: DataType,
  override val isNullable: Boolean = false
) extends MetadataColumn {
  def toStructField: StructField = StructField(name, dataType, isNullable)
}
