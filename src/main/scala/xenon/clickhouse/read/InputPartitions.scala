package xenon.clickhouse.read

import org.apache.spark.sql.connector.read.InputPartition
import xenon.clickhouse.spec.ShardSpec

case class ClickHouseInputPartition(
  shard: Option[ShardSpec],
  partition: Option[String]
) extends InputPartition {

  override def preferredLocations(): Array[String] = shard match {
    case Some(ShardSpec(_, _, replicas)) => replicas.map(_.node.host)
    case None => Array()
  }
}

object ClickHouseWholeTable extends ClickHouseInputPartition(None, None)
