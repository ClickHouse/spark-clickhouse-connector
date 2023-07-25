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

package xenon.clickhouse.write

import java.time.ZoneId
import org.apache.spark.sql.clickhouse.{ExprUtils, WriteOptions}
import org.apache.spark.sql.connector.expressions.{Expression, SortOrder, Transform}
import org.apache.spark.sql.types.StructType
import xenon.clickhouse.expr.{Expr, FuncExpr, OrderExpr}
import xenon.clickhouse.func.FunctionRegistry
import xenon.clickhouse.spec._

case class WriteJobDescription(
  queryId: String,
  tableSchema: StructType,
  metadataSchema: StructType,
  dataSetSchema: StructType,
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpec,
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpec],
  shardingKey: Option[Expr],
  partitionKey: Option[List[Expr]],
  sortingKey: Option[List[OrderExpr]],
  writeOptions: WriteOptions,
  functionRegistry: FunctionRegistry
) {

  def targetDatabase(convert2Local: Boolean): String = tableEngineSpec match {
    case dist: DistributedEngineSpec if convert2Local => dist.local_db
    case _ => tableSpec.database
  }

  def targetTable(convert2Local: Boolean): String = tableEngineSpec match {
    case dist: DistributedEngineSpec if convert2Local => dist.local_table
    case _ => tableSpec.name
  }

  def shardingKeyIgnoreRand: Option[Expr] = shardingKey filter {
    case FuncExpr("rand", Nil) => false
    case _ => true
  }

  def sparkShardExpr: Option[Expression] = shardingKeyIgnoreRand match {
    case Some(expr) => ExprUtils.toSparkTransformOpt(expr, functionRegistry)
    case _ => None
  }

  def sparkSplits: Array[Transform] =
    // Pmod by total weight * constant. Note that this key will be further hashed by spark. Reasons of doing this:
    //   - Enlarged range of modulo to avoid hash collision of small number of shards, hence mitigate data skew caused
    //     by this.
    //   - Still distribute data from one shard to only a subset of executors. If we do not apply modulo (instead we
    //     need to apply module during sorting in `toSparkSortOrders`), data belongs to shard 1 will be sorted in the
    //     front for all tasks, resulting in instant high pressure for shard 1 when stage starts.
    if (writeOptions.repartitionByPartition) {
      ExprUtils.toSparkSplits(
        shardingKeyIgnoreRand.map(k => ExprUtils.toSplitWithModulo(k, cluster.get.totalWeight * 5)),
        partitionKey,
        functionRegistry
      )
    } else {
      ExprUtils.toSparkSplits(
        shardingKeyIgnoreRand.map(k => ExprUtils.toSplitWithModulo(k, cluster.get.totalWeight * 5)),
        None,
        functionRegistry
      )
    }

  def sparkSortOrders: Array[SortOrder] = {
    val _partitionKey = if (writeOptions.localSortByPartition) partitionKey else None
    val _sortingKey = if (writeOptions.localSortByKey) sortingKey else None
    ExprUtils.toSparkSortOrders(shardingKeyIgnoreRand, _partitionKey, _sortingKey, cluster, functionRegistry)
  }
}
