package xenon.clickhouse.write

import java.time.ZoneId

import org.apache.spark.sql.types.StructType
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
  localTableEngineSpec: Option[TableEngineSpec]
  // need something like partitionSpec here, to calculate shard, partition of records.
  // but need to add an repartition to make sure RDD distribution match clickhouse firstly
)
