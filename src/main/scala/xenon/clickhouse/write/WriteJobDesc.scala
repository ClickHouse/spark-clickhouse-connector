package xenon.clickhouse.write

import java.time.ZoneId

import org.apache.spark.sql.types.StructType
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec}

case class WriteJobDesc(
  id: String,
  tz: ZoneId,
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  database: String,
  table: String,
  schema: StructType
)
