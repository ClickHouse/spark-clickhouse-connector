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

package com.clickhouse.spark

import com.clickhouse.data.ClickHouseColumn

import scala.util.Try

object ColumnUtils {

  /**
   * Parses a single ClickHouse column definition, or `None` if the client can not parse the
   * type, e.g. a type introduced by a ClickHouse server newer than the bundled client.
   */
  def tryParseColumn(name: String, chType: String): Option[ClickHouseColumn] =
    Try(ClickHouseColumn.parse(s"`$name` $chType")).toOption
      .collect { case chCols if chCols.size == 1 => chCols.get(0) }

  /** Renders `(name, type)` pairs as <code>`name` type, ...</code> for logs and table properties. */
  def renderColumns(columns: Seq[(String, String)]): String =
    columns.map { case (name, chType) => s"`$name` $chType" }.mkString(", ")
}
