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

import org.apache.spark.sql.TransformUtil._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.read.ClickHouseScanBuilder
import xenon.clickhouse.spec.{TableEngineSpec, _}
import xenon.clickhouse.write.ClickHouseWriteBuilder

import java.time.ZoneId
import java.util
import scala.collection.JavaConverters._
import scala.util.Using

class ClickHouseTable(
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  implicit val tz: Either[ZoneId, ZoneId],
  spec: TableSpec,
  engineSpec: TableEngineSpec,
  globalWriteBatchSize: Int,
  globalDistWriteUseClusterNodes: Boolean,
  globalDistReadUseClusterNodes: Boolean,
  globalDistWriteConvertToLocal: Boolean,
  globalDistReadConvertToLocal: Boolean
) extends Table
    with SupportsRead
    with SupportsWrite
    with SupportsMetadataColumns
    with ClickHouseHelper
    with Logging {

  lazy val localEngineSpec: Option[MergeTreeEngineSpec] = engineSpec match {
    case distSpec: DistributedEngineSpec => Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
        val localTableSpec = queryTableSpec(distSpec.local_db, distSpec.local_table)
        Some(TableEngineUtil.resolveTableEngine(localTableSpec).asInstanceOf[MergeTreeEngineSpec])
      }
    case _ => None
  }

  def database: String = spec.database

  def table: String = spec.name

  def isDistributed: Boolean = engineSpec.is_distributed

  def shardingKey: Option[String] = engineSpec match {
    case _spec: DistributedEngineSpec => _spec.sharding_key
    case _ => None
  }

  def sortingKey: Option[String] = engineSpec match {
    case mergeTreeFamilySpec: MergeTreeFamilyEngineSpec => Some(mergeTreeFamilySpec.sorting_key).filter(_.nonEmpty)
    case _: DistributedEngineSpec => localEngineSpec.map(_.sorting_key).filter(_.nonEmpty)
    case _: TableEngineSpec => None
  }

  def partitionKey: Option[String] = engineSpec match {
    case mergeTreeFamilySpec: MergeTreeFamilyEngineSpec => mergeTreeFamilySpec.partition_key.filter(_.nonEmpty)
    case _: DistributedEngineSpec => localEngineSpec.flatMap(_.partition_key).filter(_.nonEmpty)
    case _: TableEngineSpec => None
  }

  override def name: String = s"ClickHouse Table | ${spec.database}.${spec.name} | ${spec.engine}"

  override def capabilities(): util.Set[TableCapability] =
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava

  override lazy val schema: StructType = Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
    queryTableSchema(database, table)
  }

  override lazy val partitioning: Array[Transform] = (
    shardingKey.map(fromClickHouse).map(wrapShard).seq ++: partitionKey.map(fromClickHouse).map(wrapPartition).seq
  ).toArray

  override def metadataColumns(): Array[MetadataColumn] = Array()

  override lazy val properties: util.Map[String, String] = spec.toJavaMap

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    log.info(s"read options ${options.asScala}")
    // TODO handle read options
    new ClickHouseScanBuilder(
      node,
      cluster,
      tz,
      database,
      table,
      schema,
      globalDistReadUseClusterNodes,
      globalDistReadConvertToLocal
    )
  }

  override def newWriteBuilder(info: LogicalWriteInfo): ClickHouseWriteBuilder = {
    log.info(s"write options ${info.options.asScala}")
    // TODO handle write options info.options()
    new ClickHouseWriteBuilder(
      info.queryId(),
      node,
      cluster,
      tz,
      database,
      table,
      info.schema(),
      globalWriteBatchSize,
      globalDistWriteUseClusterNodes,
      globalDistWriteConvertToLocal
    )
  }
}
