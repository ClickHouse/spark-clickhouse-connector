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

import com.clickhouse.spark.spec.{ClusterSpec, DistributedEngineSpec, NodeSpec, TableEngineSpec, TableSpec}
import org.apache.spark.sql.clickhouse.ReadOptions
import org.apache.spark.sql.types.StructType

import java.time.ZoneId

case class ScanJobDescription(
  node: NodeSpec,
  tz: ZoneId,
  tableSpec: TableSpec,
  tableEngineSpec: TableEngineSpec,
  cluster: Option[ClusterSpec],
  localTableSpec: Option[TableSpec],
  localTableEngineSpec: Option[TableEngineSpec],
  readOptions: ReadOptions,
  // Below fields will be constructed in ScanBuilder.
  readSchema: StructType = new StructType,
  // We should pass compiled ClickHouse SQL snippets(or ClickHouse SQL AST data structure) instead of Spark Expression
  // into Scan tasks because the check happens in planing phase on driver side.
  filtersExpr: String = "1=1",
  groupByClause: Option[String] = None,
  limit: Option[Int] = None
) {

  def database: String = tableEngineSpec match {
    case dist: DistributedEngineSpec if readOptions.convertDistributedToLocal => dist.local_db
    case _ => tableSpec.database
  }

  def table: String = tableEngineSpec match {
    case dist: DistributedEngineSpec if readOptions.convertDistributedToLocal => dist.local_table
    case _ => tableSpec.name
  }
}
