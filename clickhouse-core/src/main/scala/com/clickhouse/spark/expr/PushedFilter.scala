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

package com.clickhouse.spark.expr

import com.clickhouse.spark.Utils

/** A filter pushed down to ClickHouse, compiled on the driver into a WHERE conjunct. */
sealed trait PushedFilter {
  def sql: String

  /** The columns it reads. */
  def columns: Set[String]
}

object PushedFilter {

  /** `column op value`, `op` one of `<`, `<=`, `=`, `>=`, `>` and `value` a SQL literal. */
  case class Compare(column: String, op: String, value: String) extends PushedFilter {
    def sql: String = s"${Utils.wrapBackQuote(column)} $op $value"
    def columns: Set[String] = Set(column)
  }

  /** `column IN (values)`, the values SQL literals. */
  case class In(column: String, values: Seq[String]) extends PushedFilter {
    def sql: String = s"${Utils.wrapBackQuote(column)} IN (${values.mkString(",")})"
    def columns: Set[String] = Set(column)
  }

  /** Any other filter, as the SQL it compiled to. */
  case class Other(sql: String, columns: Set[String]) extends PushedFilter
}
