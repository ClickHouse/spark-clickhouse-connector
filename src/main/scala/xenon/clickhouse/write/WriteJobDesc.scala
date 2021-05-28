package xenon.clickhouse.write

import java.time.ZoneId

import org.apache.spark.sql.types.StructType
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec, TableEngineSpec, TableSpec}

case class WriteJobDesc(
  id: String,
  schema: StructType,
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpec,
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpec]
)
