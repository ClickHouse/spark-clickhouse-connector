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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.{ClickHouseUnsupportedType, ExprUtils}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference, SortOrder => V2SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.types.StructType
import com.clickhouse.spark._
import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.expr.ExprRender
import com.clickhouse.spark.read.format.{ClickHouseBinaryReader, ClickHouseJsonReader}
import com.clickhouse.spark.read.TaskSplit._
import com.clickhouse.spark.spec._

import java.time.ZoneId
import scala.util.control.NonFatal

class ClickHouseScanBuilder(
  scanJob: ScanJobDescription,
  physicalSchema: StructType,
  metadataSchema: StructType,
  partitionTransforms: Array[Transform]
) extends ScanBuilder
    with SupportsPushDownTopN
    with SupportsPushDownLimit
    with SupportsPushDownFilters
    with SupportsPushDownAggregates
    with SupportsPushDownRequiredColumns
    with ClickHouseHelper
    with SQLHelper
    with SQLConfHelper
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

  private var _orderByClause: Option[String] = None

  override def pushTopN(orders: Array[V2SortOrder], limit: Int): Boolean = {
    if (!conf.getConf(READ_PUSHDOWN_TOP_N)) return false

    val translated = orders.map(o => ExprUtils.toClickHouseSortOrderOpt(o))
    if (translated.exists(_.isEmpty)) return false

    this._orderByClause =
      Some(translated.flatten.map(ExprRender.renderOrder).mkString("ORDER BY ", ", ", ""))
    this._limit = Some(limit)
    true
  }

  // Each ClickHouse input partition runs its own `ORDER BY ... LIMIT n`, returning a local
  // top-N. Spark must perform the final global merge across partitions, so we always
  // report the pushdown as partial.
  override def isPartiallyPushed: Boolean = true

  private var _pushedFilters = Array.empty[Filter]

  override def pushedFilters: Array[Filter] = this._pushedFilters

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unSupported) = filters.partition(f => compileFilter(f).isDefined)
    this._pushedFilters = pushed
    unSupported
  }

  private var _pushedGroupByCols: Option[Array[String]] = None
  private var _groupByClause: Option[String] = None

  /** Such aggregations must be declined - pushing them fails Spark's output-schema assertion. */
  private def referencesUnsupportedColumn(aggregation: Aggregation): Boolean = {
    val unsupportedColumnNames = ClickHouseUnsupportedType.unsupportedColumns(physicalSchema).map(_._1).toSet
    (aggregation.aggregateExpressions ++ aggregation.groupByExpressions)
      .flatMap(_.references.map(_.fieldNames.head))
      .exists(unsupportedColumnNames.contains)
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (referencesUnsupportedColumn(aggregation)) return false

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
      val queryTimeoutMs = scanJob.readOptions.clientQueryTimeout
      _readSchema = Utils.tryWithResource(NodeClient(scanJob.node, queryTimeoutMs)) { implicit nodeClient: NodeClient =>
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

  /** The pruned read schema must not contain placeholder [[ClickHouseUnsupportedType]] columns. */
  private def validateReadSchema(): Unit = {
    val unsupportedColumns = ClickHouseUnsupportedType.unsupportedColumns(_readSchema)
    if (unsupportedColumns.nonEmpty) {
      throw CHClientException(
        s"Can not read columns with unsupported ClickHouse types: " +
          s"${ColumnUtils.renderColumns(unsupportedColumns)}. " +
          "Select only the supported columns explicitly instead of referencing these columns (e.g. via SELECT *)."
      )
    }
  }

  // without GROUP BY, an empty task still returns a row of ClickHouse's defaults (not NULL) that Spark merges in
  private def globalAggregatePushed: Boolean = _pushedGroupByCols.exists(_.isEmpty)

  override def build(): Scan = {
    validateReadSchema()
    new ClickHouseBatchScan(
      scanJob.copy(
        readSchema = _readSchema,
        filtersExpr = compileFilters(AlwaysTrue :: pushedFilters.toList),
        groupByClause = _groupByClause,
        orderByClause = _orderByClause,
        limit = _limit
      ),
      globalAggregatePushed = globalAggregatePushed,
      tableColumns = physicalSchema.fieldNames.toSet,
      pushedFilters = pushedFilters
    )
  }
}

class ClickHouseBatchScan(
  scanJob: ScanJobDescription,
  globalAggregatePushed: Boolean = false,
  tableColumns: Set[String] = Set.empty,
  pushedFilters: Array[Filter] = Array.empty
) extends Scan with Batch
    with SupportsReportPartitioning
    with SupportsRuntimeFiltering
    with PartitionReaderFactory
    with ClickHouseHelper
    with SQLHelper {

  implicit private val tz: ZoneId = scanJob.tz

  private var runtimeFilters: Array[Filter] = Array.empty

  val database: String = scanJob.database
  val table: String = scanJob.table

  private val queryTimeoutMs: Long = scanJob.readOptions.clientQueryTimeout

  lazy val taskSplit: TaskSplit = TaskSplit.resolve(
    strategy = scanJob.readOptions.taskSplitStrategy,
    globalAggregatePushed = globalAggregatePushed,
    engine = scanJob.tableEngineSpec,
    convertDistributedToLocal = scanJob.readOptions.convertDistributedToLocal,
    useClusterNodesForDistributed = scanJob.readOptions.useClusterNodesForDistributed,
    primaryKeyRanges = planPrimaryKeyRanges()
  )

  lazy val inputPartitions: Array[ClickHouseInputPartition] = {
    val partitions = taskSplit match {
      case OneTask => Array(wholeTablePartition)
      case PartitionSplit if scanJob.tableEngineSpec.isInstanceOf[DistributedEngineSpec] => shardPartitions
      case PartitionSplit => tablePartitions
      case PrimaryIndexSplit(ranges) => ranges.map(range => wholeTablePartition.copy(keyRange = Some(range))).toArray
    }
    val oneTaskChosen = globalAggregatePushed || scanJob.readOptions.taskSplitStrategy == TASK_SPLIT_STRATEGY_ONE_TASK
    if (partitions.length == 1 && !oneTaskChosen) {
      log.warn(s"Reading $database.$table as a single partition, which may impact read performance")
    }
    partitions
  }

  // the whole table in one task; for a Distributed table, through the Distributed engine
  private def wholeTablePartition: ClickHouseInputPartition =
    ClickHouseInputPartition(scanJob.tableSpec, NoPartitionSpec, scanJob.readOptions.splitByPartitionId, scanJob.node)

  // one task per ClickHouse partition
  private def tablePartitions: Array[ClickHouseInputPartition] =
    Utils.tryWithResource(NodeClient(scanJob.node, queryTimeoutMs)) { implicit nodeClient: NodeClient =>
      queryPartitionSpec(database, table).map { partitionSpec =>
        // TODO pickup preferred
        ClickHouseInputPartition(scanJob.tableSpec, partitionSpec, scanJob.readOptions.splitByPartitionId, scanJob.node)
      }
    }.toArray

  // one task per shard and partition of the local tables behind a Distributed table
  private def shardPartitions: Array[ClickHouseInputPartition] = {
    val localTable = scanJob.localTableSpec.get
    scanJob.cluster.get.shards.flatMap { shard =>
      Utils.tryWithResource(NodeClient(shard.nodes.head, queryTimeoutMs)) { implicit nodeClient: NodeClient =>
        queryPartitionSpec(localTable.database, localTable.name).map { partitionSpec =>
          // TODO pickup preferred
          ClickHouseInputPartition(localTable, partitionSpec, scanJob.readOptions.splitByPartitionId, shard)
        }
      }
    }
  }

  private def planPrimaryKeyRanges(): Option[Seq[PrimaryKeyRange]] =
    Utils.tryWithResource(NodeClient(scanJob.node, queryTimeoutMs)) { implicit nodeClient: NodeClient =>
      val filters = pushedFilters.toSeq.flatMap(f => compileFilter(f).map(_ -> f.references.toSet))
      val slices = scanJob.readOptions.parallelism.getOrElse(3 * SparkSession.active.sparkContext.defaultParallelism)
      PrimaryKeyRange.plan(scanJob.tableSpec, tableColumns, filters, slices)
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
