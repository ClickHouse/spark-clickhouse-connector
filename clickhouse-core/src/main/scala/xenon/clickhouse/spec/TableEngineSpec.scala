package xenon.clickhouse.spec

import xenon.clickhouse.expr.{Expr, FieldRef, OrderExpr, TupleExpr, ZeroTupleExpr}

sealed trait TableEngineSpecV2 extends Serializable {
  def engine_expr: String
  def engine: String
  def args: Array[Expr] = Array.empty
  def sorting_key: Array[OrderExpr] = Array.empty
  def primary_key: TupleExpr = ZeroTupleExpr
  def partition_key: TupleExpr = ZeroTupleExpr
  def sampling_key: TupleExpr = ZeroTupleExpr
  def ttl: Option[String] = None // don't care about it now
  def settings: Map[String, String]
}

trait MergeTreeFamilyEngineSpecV2 extends TableEngineSpecV2

trait ReplicatedEngineSpecV2 extends MergeTreeFamilyEngineSpecV2 {
  def zk_path: String
  def replica_name: String
}

case class UnknownTableEngineSpecV2(
  engine_expr: String
) extends TableEngineSpecV2 {
  def engine: String = "Unknown"
  def settings: Map[String, String] = Map.empty
}

case class MergeTreeEngineSpecV2(
  engine_expr: String,
  var _sorting_key: Array[OrderExpr] = Array.empty,
  var _primary_key: TupleExpr = ZeroTupleExpr,
  var _partition_key: TupleExpr = ZeroTupleExpr,
  var _sampling_key: TupleExpr = ZeroTupleExpr,
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 {
  override def engine: String = "MergeTree"
  override def sorting_key: Array[OrderExpr] = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplicatedMergeTreeEngineSpecV2(
  engine_expr: String,
  zk_path: String,
  replica_name: String,
  var _sorting_key: Array[OrderExpr] = Array.empty,
  var _primary_key: TupleExpr = ZeroTupleExpr,
  var _partition_key: TupleExpr = ZeroTupleExpr,
  var _sampling_key: TupleExpr = ZeroTupleExpr,
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 with ReplicatedEngineSpecV2 {
  def engine: String = "ReplicatedMergeTree"
  override def sorting_key: Array[OrderExpr] = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplacingMergeTreeEngineSpecV2(
  engine_expr: String,
  version_column: Option[FieldRef] = None,
  var _sorting_key: Array[OrderExpr] = Array.empty,
  var _primary_key: TupleExpr = ZeroTupleExpr,
  var _partition_key: TupleExpr = ZeroTupleExpr,
  var _sampling_key: TupleExpr = ZeroTupleExpr,
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 {
  override def engine: String = "ReplacingMergeTree"
  override def sorting_key: Array[OrderExpr] = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplicatedReplacingMergeTreeEngineSpecV2(
  engine_expr: String,
  zk_path: String,
  replica_name: String,
  version_column: Option[FieldRef] = None,
  var _sorting_key: Array[OrderExpr] = Array.empty,
  var _primary_key: TupleExpr = ZeroTupleExpr,
  var _partition_key: TupleExpr = ZeroTupleExpr,
  var _sampling_key: TupleExpr = ZeroTupleExpr,
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 with ReplicatedEngineSpecV2 {
  override def engine: String = "ReplicatedReplacingMergeTree"
  override def sorting_key: Array[OrderExpr] = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class DistributedEngineSpecV2(
  engine_expr: String,
  cluster: String,
  local_db: String,
  local_table: String,
  sharding_key: Option[Expr] = None,
  var _settings: Map[String, String] = Map.empty
) extends TableEngineSpecV2 {
  override def engine: String = "Distributed"
  override def settings: Map[String, String] = _settings
}

sealed trait TableEngineSpec extends Serializable {
  def engine_expr: String
  def settings: Map[String, String]
  def is_distributed: Boolean = false
  def is_replicated: Boolean = false
}

trait MergeTreeFamilyEngineSpec extends TableEngineSpec {
  def sorting_key: String
  def partition_key: Option[String]
  def primary_key: Option[String]
  def sampling_key: Option[String]
}

trait ReplicatedEngineSpec extends TableEngineSpec { self: MergeTreeFamilyEngineSpec =>
  def zk_path: String
  def replica_name: String
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
) extends MergeTreeFamilyEngineSpec

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
) extends MergeTreeFamilyEngineSpec with ReplicatedEngineSpec {
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
) extends MergeTreeFamilyEngineSpec

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
) extends MergeTreeFamilyEngineSpec with ReplicatedEngineSpec {
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
