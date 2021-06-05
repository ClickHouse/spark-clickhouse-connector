package xenon.clickhouse.spec

case class PartitionSpec(
  partition: String,
  row_count: Long,
  size_in_bytes: Long
)

object NoPartitionSpec extends PartitionSpec("", 0, 0)
