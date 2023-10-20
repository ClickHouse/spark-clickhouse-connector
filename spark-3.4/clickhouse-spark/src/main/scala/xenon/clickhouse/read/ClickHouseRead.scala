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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.types.StructType
import xenon.clickhouse._
import xenon.clickhouse.client.NodeClient
import xenon.clickhouse.exception.CHClientException
import xenon.clickhouse.read.format.{ClickHouseBinaryReader, ClickHouseJsonReader}
import xenon.clickhouse.spec._

import java.time.ZoneId
import scala.util.control.NonFatal

class ClickHouseScanBuilder(
  scanJob: ScanJobDescription,
  physicalSchema: StructType,
  metadataSchema: StructType,
  partitionTransforms: Array[Transform]
) extends ScanBuilder
    with SupportsPushDownLimit
    with SupportsPushDownFilters
    with SupportsPushDownAggregates
    with SupportsPushDownRequiredColumns
    with ClickHouseHelper
    with SQLHelper
    with Logging {

  implicit private val tz: ZoneId = scanJob.tz

  private val reservedMetadataSchema: StructType = StructType(
    metadataSchema.dropWhile(field => physicalSchema.fields.map(_.name).contains(field.name))
  )

  private var _readSchema: StructType = StructType(
    physicalSchema.fields ++ reservedMetadataSchema.fields
  )

  private var _limit: Option[Int] = None

  override def pushLimit(limit: Int): Boolean = {
    this._limit = Some(limit)
    true
  }

  private var _pushedFilters = Array.empty[Filter]

  override def pushedFilters: Array[Filter] = this._pushedFilters

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unSupported) = filters.partition(f => compileFilter(f).isDefined)
    this._pushedFilters = pushed
    unSupported
  }

  private var _pushedGroupByCols: Option[Array[String]] = None
  private var _groupByClause: Option[String] = None

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    val compiledAggs = aggregation.aggregateExpressions.flatMap(compileAggregate)
    if (compiledAggs.length != aggregation.aggregateExpressions.length) return false

    val compiledGroupByCols = aggregation.groupByExpressions.map(_.toString)

    // The column names here are already quoted and can be used to build sql string directly.
    // e.g. [`DEPT`, `NAME`, MAX(`SALARY`), MIN(`BONUS`)] =>
    //        SELECT `DEPT`, `NAME`, MAX(`SALARY`), MIN(`BONUS`)
    //        FROM `test`.`employee`
    //        WHERE 1=0
    //        GROUP BY `DEPT`, `NAME`
    val compiledSelectItems = compiledGroupByCols ++ compiledAggs
    val groupByClause = if (compiledGroupByCols.nonEmpty) "GROUP BY " + compiledGroupByCols.mkString(", ") else ""
    val aggQuery =
      s"""SELECT ${compiledSelectItems.mkString(", ")}
         |FROM ${quoted(scanJob.tableSpec.database)}.${quoted(scanJob.tableSpec.name)}
         |WHERE 1=0
         |$groupByClause
         |""".stripMargin
    try {
      _readSchema = Utils.tryWithResource(NodeClient(scanJob.node)) { implicit nodeClient: NodeClient =>
        val fields = (getQueryOutputSchema(aggQuery) zip compiledSelectItems)
          .map { case (structField, colExpr) => structField.copy(name = colExpr) }
        StructType(fields)
      }
      _pushedGroupByCols = Some(compiledGroupByCols)
      _groupByClause = Some(groupByClause)
      true
    } catch {
      case NonFatal(e) =>
        log.error("Failed to push down aggregation to ClickHouse", e)
        false
    }
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.map(_.name)
    this._readSchema = StructType(_readSchema.filter(field => requiredCols.contains(field.name)))
  }

  override def build(): Scan = new ClickHouseBatchScan(scanJob.copy(
    readSchema = _readSchema,
    filtersExpr = compileFilters(AlwaysTrue :: pushedFilters.toList),
    groupByClause = _groupByClause,
    limit = _limit
  ))
}

class ClickHouseBatchScan(scanJob: ScanJobDescription) extends Scan with Batch
    with SupportsReportPartitioning
    with SupportsRuntimeFiltering
    with PartitionReaderFactory
    with ClickHouseHelper
    with SQLHelper {

  implicit private val tz: ZoneId = scanJob.tz

  private var runtimeFilters: Array[Filter] = Array.empty

  val database: String = scanJob.database
  val table: String = scanJob.table

  lazy val inputPartitions: Array[ClickHouseInputPartition] = scanJob.tableEngineSpec match {
    case DistributedEngineSpec(_, _, local_db, local_table, _, _) if scanJob.readOptions.convertDistributedToLocal =>
      scanJob.cluster.get.shards.flatMap { shardSpec =>
        Utils.tryWithResource(NodeClient(shardSpec.nodes.head)) { implicit nodeClient: NodeClient =>
          queryPartitionSpec(local_db, local_table).map { partitionSpec =>
            ClickHouseInputPartition(
              scanJob.localTableSpec.get,
              partitionSpec,
              scanJob.readOptions.splitByPartitionId,
              shardSpec // TODO pickup preferred
            )
          }
        }
      }
    case _: DistributedEngineSpec if scanJob.readOptions.useClusterNodesForDistributed =>
      throw CHClientException(
        s"${READ_DISTRIBUTED_USE_CLUSTER_NODES.key} is not supported yet."
      )
    case _: DistributedEngineSpec =>
      // we can not collect all partitions from single node, thus should treat table as no partitioned table
      Array(ClickHouseInputPartition(
        scanJob.tableSpec,
        NoPartitionSpec,
        scanJob.readOptions.splitByPartitionId,
        scanJob.node
      ))
    case _: TableEngineSpec =>
      Utils.tryWithResource(NodeClient(scanJob.node)) { implicit nodeClient: NodeClient =>
        queryPartitionSpec(database, table).map { partitionSpec =>
          ClickHouseInputPartition(
            scanJob.tableSpec,
            partitionSpec,
            scanJob.readOptions.splitByPartitionId,
            scanJob.node // TODO pickup preferred
          )
        }
      }.toArray
  }

  override def toBatch: Batch = this

  // may contains meta columns
  override def readSchema(): StructType = scanJob.readSchema

  override def planInputPartitions: Array[InputPartition] = inputPartitions.toArray

  // TODO KeyGroupedPartitioning
  override def outputPartitioning(): Partitioning = new UnknownPartitioning(inputPartitions.length)

  override def createReaderFactory: PartitionReaderFactory = this

  override def createReader(_partition: InputPartition): PartitionReader[InternalRow] = {
    val format = scanJob.readOptions.format
    val partition = _partition.asInstanceOf[ClickHouseInputPartition]
    val finalScanJob = scanJob.copy(filtersExpr =
      scanJob.filtersExpr + " AND "
        + compileFilters(AlwaysTrue :: runtimeFilters.toList)
    )
    format match {
      case "json" => new ClickHouseJsonReader(finalScanJob, partition)
      case "binary" => new ClickHouseBinaryReader(finalScanJob, partition)
      case unsupported => throw CHClientException(s"Unsupported read format: $unsupported")
    }
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = Array(
    BlocksReadMetric(),
    BytesReadMetric()
  )

  override def filterAttributes(): Array[NamedReference] =
    if (scanJob.readOptions.runtimeFilterEnabled) {
      scanJob.readSchema.fields.map(field => Expressions.column(field.name))
    } else {
      Array.empty
    }

  override def filter(filters: Array[Filter]): Unit =
    runtimeFilters = filters
}
