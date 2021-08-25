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
import scala.util.Using

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.grpc.GrpcNodeClient
import xenon.clickhouse.read.{ClickHouseScanBuilder, ScanJobDesc}
import xenon.clickhouse.spec._
import xenon.clickhouse.write.{ClickHouseWriteBuilder, WriteJobDesc}
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

  override def metadataColumns(): Array[MetadataColumn] = Array()

  override lazy val properties: util.Map[String, String] = spec.toJavaMap

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    log.info(s"read options ${options.asScala}")
    // TODO handle read options

    val jobDesc = ScanJobDesc(
      node = node,
      tz = tz,
      tableSpec = spec,
      tableEngineSpec = engineSpec,
      cluster = cluster,
      localTableSpec = localTableSpec,
      localTableEngineSpec = localTableEngineSpec
    )
    // TODO schema of meta columns, partitions
    new ClickHouseScanBuilder(jobDesc, schema, new StructType(), Array())
  }

  override def newWriteBuilder(info: LogicalWriteInfo): ClickHouseWriteBuilder = {
    log.info(s"write options ${info.options.asScala}")
    // TODO handle write options info.options()

    val jobDesc = WriteJobDesc(
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

    new ClickHouseWriteBuilder(jobDesc)
  }
}
