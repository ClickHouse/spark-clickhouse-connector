package xenon.clickhouse.read

import java.time.ZoneId

import org.apache.spark.sql.types.StructType
import xenon.clickhouse.spec._

case class ScanJobDesc(
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpecV2,
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpecV2],
  // below fields will be constructed in ScanBuilder
  readSchema: StructType = new StructType,
  filterExpr: String = "1=1"
)
