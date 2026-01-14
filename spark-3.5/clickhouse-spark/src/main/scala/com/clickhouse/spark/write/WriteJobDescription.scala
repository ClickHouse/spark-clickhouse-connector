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

package com.clickhouse.spark.write

import com.clickhouse.spark.expr.{Expr, FuncExpr, OrderExpr}
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.func.FunctionRegistry
import com.clickhouse.spark.spec.{ClusterSpec, DistributedEngineSpec, NodeSpec, TableEngineSpec, TableSpec}

import java.time.ZoneId
import org.apache.spark.sql.clickhouse.{ExprUtils, WriteOptions}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.IGNORE_UNSUPPORTED_TRANSFORM
import org.apache.spark.sql.connector.expressions.{Expression, SortOrder, Transform}
import org.apache.spark.sql.types.StructType
import com.clickhouse.spark.spec._

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

  // For SharedMergeTree, filter out unsupported partition expressions while keeping supported ones.
  private lazy val isSharedMergeTree: Boolean =
    tableEngineSpec.engine_clause.toLowerCase.contains("sharedmergetree")

  private def filterSupportedPartitionExprs(exprs: Option[List[Expr]]): Option[List[Expr]] = {
    if (!isSharedMergeTree) return exprs
    exprs.map(_.flatMap { expr =>
      ExprUtils.toSparkTransformOpt(expr, functionRegistry).map(_ => expr)
    })
  }

  def sparkSplits: Array[Transform] = {
    val _partitionKey = if (writeOptions.repartitionByPartition) {
      filterSupportedPartitionExprs(partitionKey)
    } else {
      None
    }
    ExprUtils.toSparkSplits(
      shardingKeyIgnoreRand,
      _partitionKey,
      functionRegistry
    )
  }

  def sparkSortOrders: Array[SortOrder] = {
    val _partitionKey = if (writeOptions.localSortByPartition) {
      filterSupportedPartitionExprs(partitionKey)
    } else {
      None
    }
    val _sortingKey = if (writeOptions.localSortByKey) sortingKey else None
    ExprUtils.toSparkSortOrders(
      shardingKeyIgnoreRand,
      _partitionKey,
      _sortingKey,
      cluster,
      functionRegistry
    )
  }

  def validateDistributedTableSharding(): Unit = {
    tableEngineSpec match {
      case _: DistributedEngineSpec if writeOptions.convertDistributedToLocal =>
        val ignoreUnsupported = writeOptions.conf.getConf(IGNORE_UNSUPPORTED_TRANSFORM)
        
        if (ignoreUnsupported) {
          val shardingKeySupported = shardingKeyIgnoreRand match {
            case Some(expr) => ExprUtils.toSparkTransformOpt(expr, functionRegistry).isDefined
            case None => true
          }
          
          if (!shardingKeySupported && !writeOptions.allowUnsupportedShardingWithConvertLocal) {
            throw CHClientException(
              s"Writing to Distributed table with unsupported sharding key while " +
                s"`spark.clickhouse.write.distributed.convertLocal=true` and " +
                s"`spark.clickhouse.ignoreUnsupportedTransform=true` may cause data corruption " +
                s"due to incorrect sharding. " +
                s"To allow this dangerous combination, set " +
                s"`spark.clickhouse.write.distributed.convertLocal.allowUnsupportedSharding=true`. " +
                s"Sharding key: ${shardingKeyIgnoreRand.getOrElse("none")}"
            )
          }
        }
      case _ =>
    }
  }
}
