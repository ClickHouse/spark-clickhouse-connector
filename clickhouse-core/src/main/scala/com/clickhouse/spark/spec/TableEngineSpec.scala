/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.spark.spec

import com.clickhouse.spark.expr.{Expr, FieldRef, OrderExpr, TupleExpr}
import com.clickhouse.spark.expr._

sealed trait TableEngineSpec extends Serializable {
  def engine_clause: String
  def engine: String
  def args: List[Expr] = List.empty
  def sorting_key: TupleExpr = TupleExpr(List.empty)
  def primary_key: TupleExpr = TupleExpr(List.empty)
  def partition_key: TupleExpr = TupleExpr(List.empty)
  def sampling_key: TupleExpr = TupleExpr(List.empty)
  def ttl: Option[String] = None // don't care about it now
  def settings: Map[String, String]
  def is_distributed: Boolean = false
  def is_replicated: Boolean = false
  def order_by_expr: List[OrderExpr] = sorting_key.exprList.map(OrderExpr(_))
}

trait MergeTreeFamilyEngineSpec extends TableEngineSpec

trait ReplicatedEngineSpec extends MergeTreeFamilyEngineSpec {
  def zk_path: String
  def replica_name: String
  override def is_replicated: Boolean = true
}

case class UnknownTableEngineSpec(
  engine_clause: String
) extends TableEngineSpec {
  def engine: String = "Unknown"
  def settings: Map[String, String] = Map.empty
}

case class MergeTreeEngineSpec(
  engine_clause: String,
  var _sorting_key: TupleExpr = TupleExpr(List.empty),
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpec {
  override def engine: String = "MergeTree"
  override def sorting_key: TupleExpr = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplicatedMergeTreeEngineSpec(
  engine_clause: String,
  zk_path: String,
  replica_name: String,
  var _sorting_key: TupleExpr = TupleExpr(List.empty),
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpec with ReplicatedEngineSpec {
  def engine: String = "ReplicatedMergeTree"
  override def sorting_key: TupleExpr = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplacingMergeTreeEngineSpec(
  engine_clause: String,
  version_column: Option[FieldRef] = None,
  var _sorting_key: TupleExpr = TupleExpr(List.empty),
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpec {
  override def engine: String = "ReplacingMergeTree"
  override def sorting_key: TupleExpr = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class ReplicatedReplacingMergeTreeEngineSpec(
  engine_clause: String,
  zk_path: String,
  replica_name: String,
  version_column: Option[FieldRef] = None,
  var _sorting_key: TupleExpr = TupleExpr(List.empty),
  var _primary_key: TupleExpr = TupleExpr(List.empty),
  var _partition_key: TupleExpr = TupleExpr(List.empty),
  var _sampling_key: TupleExpr = TupleExpr(List.empty),
  var _ttl: Option[String] = None,
  var _settings: Map[String, String] = Map.empty
) extends MergeTreeFamilyEngineSpec with ReplicatedEngineSpec {
  override def engine: String = "ReplicatedReplacingMergeTree"
  override def sorting_key: TupleExpr = _sorting_key
  override def primary_key: TupleExpr = _primary_key
  override def partition_key: TupleExpr = _partition_key
  override def sampling_key: TupleExpr = _sampling_key
  override def ttl: Option[String] = _ttl
  override def settings: Map[String, String] = _settings
}

case class DistributedEngineSpec(
  engine_clause: String,
  cluster: String,
  local_db: String,
  local_table: String,
  sharding_key: Option[Expr] = None,
  var _settings: Map[String, String] = Map.empty
) extends TableEngineSpec {
  override def engine: String = "Distributed"
  override def settings: Map[String, String] = _settings
  override def is_distributed: Boolean = true
}
