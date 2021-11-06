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

package xenon.clickhouse

import java.time.ZoneId
import java.util

import scala.collection.JavaConverters._
import scala.util.Using

import org.apache.spark.sql.clickhouse.ExprUtils
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.grpc.GrpcNodeClient
import xenon.clickhouse.read.{ClickHouseMetadataColumn, ClickHouseScanBuilder, ScanJobDescription}
import xenon.clickhouse.spec._
import xenon.clickhouse.write.{ClickHouseWriteBuilder, WriteJobDescription}
import xenon.clickhouse.Utils._
import xenon.clickhouse.expr.{Expr, OrderExpr}

class ClickHouseTable(
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  implicit val tz: ZoneId,
  spec: TableSpec,
  engineSpec: TableEngineSpec
) extends Table
    with SupportsRead
    with SupportsWrite
    with SupportsMetadataColumns
    with ClickHouseHelper
    with Logging {

  def database: String = spec.database

  def table: String = spec.name

  def isDistributed: Boolean = engineSpec.is_distributed

  lazy val (localTableSpec, localTableEngineSpec): (Option[TableSpec], Option[MergeTreeFamilyEngineSpec]) =
    engineSpec match {
      case distSpec: DistributedEngineSpec => Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
          val _localTableSpec = queryTableSpec(distSpec.local_db, distSpec.local_table)
          val _localTableEngineSpec =
            TableEngineUtils.resolveTableEngine(_localTableSpec).asInstanceOf[MergeTreeFamilyEngineSpec]
          (Some(_localTableSpec), Some(_localTableEngineSpec))
        }
      case _ => (None, None)
    }

  def shardingKey: Option[Expr] = engineSpec match {
    case _spec: DistributedEngineSpec => _spec.sharding_key
    case _ => None
  }

  def partitionKey: Option[List[Expr]] = engineSpec match {
    case mergeTreeFamilySpec: MergeTreeFamilyEngineSpec => Some(mergeTreeFamilySpec.partition_key.exprList)
    case _: DistributedEngineSpec => localTableEngineSpec.map(_.partition_key.exprList)
    case _: TableEngineSpec => None
  }

  def sortingKey: Option[List[OrderExpr]] = engineSpec match {
    case mergeTreeFamilySpec: MergeTreeFamilyEngineSpec => Some(mergeTreeFamilySpec.sorting_key).filter(_.nonEmpty)
    case _: DistributedEngineSpec => localTableEngineSpec.map(_.sorting_key).filter(_.nonEmpty)
    case _: TableEngineSpec => None
  }

  override def name: String = s"${wrapBackQuote(spec.database)}.${wrapBackQuote(spec.name)}"

  override def capabilities(): util.Set[TableCapability] =
    Set(
      BATCH_READ,
      BATCH_WRITE,
      TRUNCATE,
      ACCEPT_ANY_SCHEMA // TODO check schema and handle extra column before write
    ).asJava

  override lazy val schema: StructType = Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
    queryTableSchema(database, table)
  }

  override lazy val partitioning: Array[Transform] = ExprUtils.toSparkParts(shardingKey, partitionKey)

  /**
   * Only support `MergeTree` and `Distributed` table engine, for reference
   * {{{NamesAndTypesList MergeTreeData::getVirtuals()}}}, {{{NamesAndTypesList StorageDistributed::getVirtuals()}}}
   */
  override def metadataColumns(): Array[ClickHouseMetadataColumn] = engineSpec match {
    case _: MergeTreeFamilyEngineSpec =>
      Array(
        ClickHouseMetadataColumn("_part", StringType, false),
        ClickHouseMetadataColumn("_part_index", LongType, false),
        ClickHouseMetadataColumn("_part_uuid", StringType, false),
        ClickHouseMetadataColumn("_partition_id", StringType, false),
        // ClickHouseMetadataColumn("_partition_value", StringType, false),
        ClickHouseMetadataColumn("_sample_factor", DoubleType, false)
      )
    case _: DistributedEngineSpec =>
      Array(
        ClickHouseMetadataColumn("_table", StringType, false),
        ClickHouseMetadataColumn("_part", StringType, false),
        ClickHouseMetadataColumn("_part_index", LongType, false),
        ClickHouseMetadataColumn("_part_uuid", StringType, false),
        ClickHouseMetadataColumn("_partition_id", StringType, false),
        ClickHouseMetadataColumn("_sample_factor", DoubleType, false),
        ClickHouseMetadataColumn("_shard_num", IntegerType, false)
      )
  }

  override lazy val properties: util.Map[String, String] = spec.toJavaMap

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    if (options.asScala.nonEmpty) {
      // TODO handle read options
      log.warn(s"Ignored read options ${options.asScala}")
    }

    val scanJob = ScanJobDescription(
      node = node,
      tz = tz,
      tableSpec = spec,
      tableEngineSpec = engineSpec,
      cluster = cluster,
      localTableSpec = localTableSpec,
      localTableEngineSpec = localTableEngineSpec
    )
    val metadataSchema = StructType(metadataColumns().map(_.toStructField))
    // TODO schema of partitions
    val partTransforms = Array[Transform]()
    new ClickHouseScanBuilder(scanJob, schema, metadataSchema, partTransforms)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): ClickHouseWriteBuilder = {
    if (info.options.asScala.nonEmpty) {
      // TODO handle write options info.options()
      log.warn(s"Ignored write options ${info.options.asScala}")
    }

    val writeJob = WriteJobDescription(
      queryId = info.queryId,
      dataSetSchema = info.schema,
      node = node,
      tz = tz,
      tableSpec = spec,
      tableEngineSpec = engineSpec,
      cluster = cluster,
      localTableSpec = localTableSpec,
      localTableEngineSpec = localTableEngineSpec,
      shardingKey = shardingKey,
      partitionKey = partitionKey,
      sortingKey = sortingKey
    )

    new ClickHouseWriteBuilder(writeJob)
  }
}
