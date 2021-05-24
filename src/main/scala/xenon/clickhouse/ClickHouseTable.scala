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

package xenon.clickhouse

import java.time.ZoneId
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.ClickHouseAnalysisException
import xenon.clickhouse.Utils.om
import xenon.clickhouse.format.JSONOutput
import xenon.clickhouse.read.ClickHouseScanBuilder
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec, TableSpec}
import xenon.clickhouse.write.ClickHouseWriteBuilder
import org.apache.spark.sql.TransformUtil._

class ClickHouseTable(
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  preferLocalTable: Boolean,
  tz: Either[ZoneId, ZoneId],
  spec: TableSpec
) extends Table
    with SupportsRead
    with SupportsWrite
    with SupportsMetadataColumns
    with ClickHouseHelper
    with Logging {

  def database: String = spec.database
  def table: String = spec.name

  def isDistributed: Boolean = spec.isInstanceOf

  override def name: String = s"ClickHouse | ${spec.database}.${spec.name} | ${spec.engine}"

  override def capabilities(): util.Set[TableCapability] =
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava

  override lazy val schema: StructType = Using.resource(GrpcNodeClient(node)) { grpcClient =>
    val columnResult = grpcClient.syncQueryAndCheck(
      s""" SELECT
         |   `database`,                -- String
         |   `table`,                   -- String
         |   `name`,                    -- String
         |   `type`,                    -- String
         |   `position`,                -- UInt64
         |   `default_kind`,            -- String
         |   `default_expression`,      -- String
         |   `data_compressed_bytes`,   -- UInt64
         |   `data_uncompressed_bytes`, -- UInt64
         |   `marks_bytes`,             -- UInt64
         |   `comment`,                 -- String
         |   `is_in_partition_key`,     -- UInt8
         |   `is_in_sorting_key`,       -- UInt8
         |   `is_in_primary_key`,       -- UInt8
         |   `is_in_sampling_key`,      -- UInt8
         |   `compression_codec`        -- String
         | FROM `system`.`columns`
         | WHERE `database`='$database' AND `table`='$table'
         | ORDER BY `position` ASC
         | """.stripMargin
    )
    val columnOutput = om.readValue[JSONOutput](columnResult.getOutput)
    SchemaUtil.fromClickHouseSchema(columnOutput.data.map { row =>
      val fieldName = row.get("name").asText
      val ckType = row.get("type").asText
      (fieldName, ckType)
    })
  }

  // engine match {
  //   case Distributed => ShardTransform :: PartitionTransforms
  //   case _           => PartitionTransforms
  // }
  override lazy val partitioning: Array[Transform] = {
    val partitionBuilder = new ArrayBuffer[Transform]
    if (isDistributed) {
      throw ClickHouseAnalysisException("Distributed not supported yet")
    }

    if (spec.partition_key.nonEmpty) {
      partitionBuilder += fromClickHouse(spec.partition_key)
    }
    partitionBuilder.toArray
  }

  override def metadataColumns(): Array[MetadataColumn] = Array()

  override lazy val properties: util.Map[String, String] = spec.toJavaMap

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    log.info(s"read options ${options.asScala}")
    // TODO handle read options
    new ClickHouseScanBuilder(node, cluster, tz, database, table, schema)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): ClickHouseWriteBuilder = {
    log.info(s"write options ${info.options.asScala}")
    // TODO handle write options info.options()
    new ClickHouseWriteBuilder(info.queryId(), node, cluster, tz, database, table, info.schema())
  }
}
