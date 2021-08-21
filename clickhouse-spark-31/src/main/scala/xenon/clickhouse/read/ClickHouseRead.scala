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

import scala.util.Using

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import xenon.clickhouse.{ClickHouseHelper, Logging, SQLHelper}
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.grpc.GrpcNodeClient
import xenon.clickhouse.spec._

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
    with SupportsPushDownRequiredColumns
    with SQLHelper
    with Logging {

  implicit private val tz: ZoneId = jobDesc.tz

  // default read not include meta columns, like _shard_num UInt32 of Distributed tables.
  private var readSchema: StructType = physicalSchema.copy()

  private var _pushedFilters = Array.empty[Filter]

  override def pushedFilters: Array[Filter] = this._pushedFilters

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unSupported) = filters.partition(f => compileFilter(f).isDefined)
    this._pushedFilters = pushed
    unSupported
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this.readSchema = StructType(physicalSchema.filter(field => requiredCols.contains(field.name)))
  }

  override def build(): Scan = new ClickHouseBatchScan(jobDesc.copy(
    readSchema = readSchema,
    filterExpr = filterWhereClause(AlwaysTrue :: pushedFilters.toList)
  ))
}

class ClickHouseBatchScan(jobDesc: ScanJobDesc) extends Scan with Batch
    with SupportsReportPartitioning
    with PartitionReaderFactory
    with ClickHouseHelper
    with SQLConfHelper {

  val readDistributedUseClusterNodes: Boolean = conf.getConf(READ_DISTRIBUTED_USE_CLUSTER_NODES)
  val readDistributedConvertLocal: Boolean = conf.getConf(READ_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if readDistributedConvertLocal => dist.local_db
    case _ => jobDesc.tableSpec.database
  }

  val table: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if readDistributedConvertLocal => dist.local_table
    case _ => jobDesc.tableSpec.name
  }

  lazy val inputPartitions: Array[ClickHouseInputPartition] = jobDesc.tableEngineSpec match {
    case DistributedEngineSpec(_, _, local_db, local_table, _, _) if readDistributedConvertLocal =>
      jobDesc.cluster.get.shards.flatMap { shardSpec =>
        Using.resource(GrpcNodeClient(shardSpec.nodes.head)) { implicit grpcNodeClient: GrpcNodeClient =>
          queryPartitionSpec(local_db, local_table).map(partitionSpec =>
            ClickHouseInputPartition(jobDesc.localTableSpec.get, partitionSpec, shardSpec) // TODO pickup preferred
          )
        }
      }
    case _: DistributedEngineSpec if readDistributedUseClusterNodes =>
      throw ClickHouseClientException(
        s"${READ_DISTRIBUTED_USE_CLUSTER_NODES.key} is not supported yet."
      )
    case _: DistributedEngineSpec =>
      // we can not collect all partitions from single node, thus should treat table as no partitioned table
      Array(ClickHouseInputPartition(jobDesc.tableSpec, NoPartitionSpec, jobDesc.node))
    case _: TableEngineSpec =>
      Using.resource(GrpcNodeClient(jobDesc.node)) { implicit grpcNodeClient: GrpcNodeClient =>
        queryPartitionSpec(database, table).map(partitionSpec =>
          ClickHouseInputPartition(jobDesc.tableSpec, partitionSpec, jobDesc.node) // TODO pickup preferred
        )
      }.toArray
  }

  override def toBatch: Batch = this

  // may contains meta columns
  override def readSchema(): StructType = jobDesc.readSchema

  override def planInputPartitions: Array[InputPartition] = inputPartitions.toArray

  override def outputPartitioning(): Partitioning = ClickHousePartitioning(inputPartitions)

  override def createReaderFactory: PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new ClickHouseReader(jobDesc, partition.asInstanceOf[ClickHouseInputPartition])

  override def supportColumnarReads(partition: InputPartition): Boolean = false

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    super.createColumnarReader(partition)
}
