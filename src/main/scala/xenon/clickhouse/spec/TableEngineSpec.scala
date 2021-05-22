package xenon.clickhouse.spec

sealed trait TableEngineSpec {
  def engine_expr: String
  def settings: Map[String, String]
  def is_distributed: Boolean = false
  def is_replicated: Boolean = false
}

case class UnknownTableEngineSpec(
  engine_expr: String
) extends TableEngineSpec {
  def settings: Map[String, String] = Map.empty
}

case class MergeTreeEngineSpec(
  engine_expr: String,
  sorting_key: String = "uninitialized",
  partition_key: Option[String] = None,
  primary_key: Option[String] = None,
  sampling_key: Option[String] = None,
  ttl_expr: Option[String] = None,
  settings: Map[String, String] = Map.empty
) extends TableEngineSpec

case class ReplicatedMergeTreeEngineSpec(
  engine_expr: String,
  zk_path: String,
  replica_name: String,
  sorting_key: String = "uninitialized",
  partition_key: Option[String] = None,
  primary_key: Option[String] = None,
  sampling_key: Option[String] = None,
  ttl_expr: Option[String] = None,
  settings: Map[String, String] = Map.empty
) extends TableEngineSpec {
  override def is_replicated: Boolean = true
}

case class ReplacingMergeTreeEngineSpec(
  engine_expr: String,
  version_column: Option[String] = None,
  sorting_key: String = "uninitialized",
  partition_key: Option[String] = None,
  primary_key: Option[String] = None,
  sampling_key: Option[String] = None,
  settings: Map[String, String] = Map.empty
) extends TableEngineSpec

case class ReplicatedReplacingMergeTreeEngineSpec(
  engine_expr: String,
  zk_path: String,
  replica_name: String,
  version_column: Option[String] = None,
  sorting_key: String = "uninitialized",
  partition_key: Option[String] = None,
  primary_key: Option[String] = None,
  sampling_key: Option[String] = None,
  settings: Map[String, String] = Map.empty
) extends TableEngineSpec {
  override def is_replicated: Boolean = true
}

case class DistributedEngineSpec(
  engine_expr: String,
  cluster: String,
  local_db: String,
  local_table: String,
  sharding_key: Option[String] = None,
  storage_policy: Option[String] = None,
  settings: Map[String, String] = Map.empty
) extends TableEngineSpec {
  override def is_distributed: Boolean = true
}
