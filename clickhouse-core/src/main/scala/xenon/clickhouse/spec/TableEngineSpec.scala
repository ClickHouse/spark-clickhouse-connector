package xenon.clickhouse.spec

import xenon.clickhouse.expr._

sealed trait TableEngineSpecV2 extends Serializable {
  def engine_expr: String
  def engine: String
  def args: List[Expr] = List.empty
  def sorting_key: List[OrderExpr] = List.empty
  def primary_key: TupleExpr = TupleExpr(List.empty)
  def partition_key: TupleExpr = TupleExpr(List.empty)
  def sampling_key: TupleExpr = TupleExpr(List.empty)
  def ttl: Option[String] = None // don't care about it now
  def settings: Map[String, String]
  def is_distributed: Boolean = false
  def is_replicated: Boolean = false
}

trait MergeTreeFamilyEngineSpecV2 extends TableEngineSpecV2

trait ReplicatedEngineSpecV2 extends MergeTreeFamilyEngineSpecV2 {
  def zk_path: String
  def replica_name: String
  override def is_replicated: Boolean = true
}

case class UnknownTableEngineSpecV2(
  engine_expr: String
) extends TableEngineSpecV2 {
  def engine: String = "Unknown"
  def settings: Map[String, String] = Map.empty
}

case class MergeTreeEngineSpecV2(
  engine_expr: String,
  var _sorting_key: List[OrderExpr] = List.empty,
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 {
  override def engine: String = "MergeTree"
  override def sorting_key: List[OrderExpr] = _sorting_key
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
  var _sorting_key: List[OrderExpr] = List.empty,
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 with ReplicatedEngineSpecV2 {
  def engine: String = "ReplicatedMergeTree"
  override def sorting_key: List[OrderExpr] = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplacingMergeTreeEngineSpecV2(
  engine_expr: String,
  version_column: Option[FieldRef] = None,
  var _sorting_key: List[OrderExpr] = List.empty,
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 {
  override def engine: String = "ReplacingMergeTree"
  override def sorting_key: List[OrderExpr] = _sorting_key
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
  var _sorting_key: List[OrderExpr] = List.empty,
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpecV2 with ReplicatedEngineSpecV2 {
  override def engine: String = "ReplicatedReplacingMergeTree"
  override def sorting_key: List[OrderExpr] = _sorting_key
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
  override def is_distributed: Boolean = false
}
