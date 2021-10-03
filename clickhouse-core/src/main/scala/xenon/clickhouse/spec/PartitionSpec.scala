package xenon.clickhouse.spec

import xenon.clickhouse.ToJson

case class PartitionSpec(
  partition: String,
  row_count: Long,
  size_in_bytes: Long
) extends ToJson

object NoPartitionSpec extends PartitionSpec("", 0, 0)
