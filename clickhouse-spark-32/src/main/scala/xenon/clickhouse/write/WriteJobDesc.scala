package xenon.clickhouse.write

import java.time.ZoneId
import org.apache.spark.sql.types.StructType
import xenon.clickhouse.expr.{Expr, OrderExpr}
import xenon.clickhouse.spec._

case class WriteJobDesc(
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
)
