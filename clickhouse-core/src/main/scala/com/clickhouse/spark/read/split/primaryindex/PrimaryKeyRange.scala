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

/**
 * A `[lower, upper)` range of a primary key prefix, compared lexicographically. `key` holds the entries as a task's
 * filter writes them: a back-quoted column, or an expression in parentheses. A missing bound is open-ended, so the ranges
 * planned for a table cover every key value exactly once.
 */
case class PrimaryKeyRange(key: Seq[String], lower: Option[Seq[String]], upper: Option[Seq[String]]) {

  def filterExpr: String = {
    val bounds = lower.map(bound(_, ">", ">=")).toSeq ++ upper.map(bound(_, "<", "<"))
    if (bounds.isEmpty) "1=1" else bounds.map(b => s"($b)").mkString(" AND ")
  }

  // expanded into `k1 > v1 OR (k1 = v1 AND ...)`, as ClickHouse's key analysis ignores tuple comparisons
  private def bound(values: Seq[String], op: String, lastOp: String): String = {
    def render(terms: List[(String, String)]): String = terms match {
      case (entry, value) :: Nil => s"$entry $lastOp $value"
      case (entry, value) :: rest =>
        val inner = if (rest.length > 1) s"(${render(rest)})" else render(rest)
        s"$entry $op $value OR ($entry = $value AND $inner)"
    }
    render(key.zip(values).toList)
  }
}
