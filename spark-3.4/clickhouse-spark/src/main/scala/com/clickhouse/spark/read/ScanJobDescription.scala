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

package com.clickhouse.spark.read

import com.clickhouse.spark.expr.PushedFilter
import com.clickhouse.spark.func.FunctionRegistry
import com.clickhouse.spark.spec.{ClusterSpec, NodeSpec, TableEngineSpec, TableSpec}
import org.apache.spark.sql.clickhouse.ReadOptions
import org.apache.spark.sql.types.StructType

import java.time.ZoneId

case class ScanJobDescription(
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpec,
  tableColumns: Map[String, String], // every column with its ClickHouse type
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpec],
  readOptions: ReadOptions,
  maxParallelism: Int, // the most input partitions a split makes
  functionRegistry: FunctionRegistry,
  // Below fields will be constructed in ScanBuilder.
  readSchema: StructType = new StructType,
  // Pushed down as compiled ClickHouse SQL: the executors can't translate Spark expressions.
  filters: Seq[PushedFilter] = Nil,
  pushedGroupBy: Option[Seq[String]] = None, // the GROUP BY columns of a pushed aggregation, none without GROUP BY
  orderByClause: Option[String] = None,
  limit: Option[Int] = None
) {

  /** The filters as one WHERE conjunct, `1=1` without any. */
  def filtersExpr: String = ("1=1" +: filters.map(_.sql)).map(f => s"($f)").mkString(" AND ")

  def groupByClause: String = pushedGroupBy.filter(_.nonEmpty).fold("")(_.mkString("GROUP BY ", ", ", ""))

  // an empty task of such an aggregation returns a row of ClickHouse's defaults (not NULL) that Spark would merge in
  def globalAggregatePushed: Boolean = pushedGroupBy.exists(_.isEmpty)
}
