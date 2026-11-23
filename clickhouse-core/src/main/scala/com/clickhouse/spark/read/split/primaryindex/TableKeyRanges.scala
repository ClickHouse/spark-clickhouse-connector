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

package com.clickhouse.spark.read.split.primaryindex

import com.clickhouse.spark.spec.{Nodes, TableSpec}

/** Why a table can't be split by its primary key, with the failure when planning threw. */
case class NotSplittable(reason: String, cause: Option[Throwable] = None)

/** The key ranges `table` is read in, one task each from one of `nodes`, or why it is read in one task. */
case class TableKeyRanges(table: TableSpec, nodes: Nodes, ranges: Either[NotSplittable, Seq[PrimaryKeyRange]]) {

  /** The filter of every task of the table: one per range, or `1=1` for the whole of it. */
  def filterExprs: Seq[String] = ranges.fold(_ => Seq("1=1"), _.map(_.filterExpr))
}
