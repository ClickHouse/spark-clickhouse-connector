package xenon.clickhouse.read

import java.time.ZoneId

import org.apache.spark.sql.types.StructType
import xenon.clickhouse.spec._

case class ScanJobDescription(
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpec,
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpec],
  // below fields will be constructed in ScanBuilder
  readSchema: StructType = new StructType,
  filterExpr: String = "1=1"
)
