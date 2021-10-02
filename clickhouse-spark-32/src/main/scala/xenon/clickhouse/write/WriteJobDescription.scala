package xenon.clickhouse.write

import java.time.ZoneId

import org.apache.spark.sql.clickhouse.ExprUtils
import org.apache.spark.sql.clickhouse.TransformUtils.toSparkTransform
import org.apache.spark.sql.connector.expressions.{Expression, SortOrder, Transform}
import org.apache.spark.sql.types.StructType
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.expr.{Expr, OrderExpr}
import xenon.clickhouse.spec._

case class WriteJobDescription(
  queryId: String,
  dataSetSchema: StructType,
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpec,
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpec],
  shardingKey: Option[Expr],
  partitionKey: Option[List[Expr]],
  sortingKey: Option[List[OrderExpr]]
) {

  def targetDatabase(convert2Local: Boolean): String = tableEngineSpec match {
    case dist: DistributedEngineSpec if convert2Local => dist.local_db
    case _ => tableSpec.database
  }

  def targetTable(convert2Local: Boolean): String = tableEngineSpec match {
    case dist: DistributedEngineSpec if convert2Local => dist.local_table
    case _ => tableSpec.name
  }

  def sparkShardExpr: Option[Expression] = (tableEngineSpec, shardingKey) match {
    case (_: DistributedEngineSpec, Some(expr)) => Some(toSparkTransform(expr))
    case (_: DistributedEngineSpec, None) =>
      throw ClickHouseClientException("Can not write data to a Distributed table that lacks sharding keys")
    case _ => None
  }

  def sparkParts: Array[Transform] = ExprUtils.toSparkParts(shardingKey, partitionKey)

  def sparkSortOrders: Array[SortOrder] = ExprUtils.toSparkSortOrders(sortingKey)
}
