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

import com.clickhouse.spark.expr.PushedFilter

/**
 * A pushed filter as conditions on the index rows, which hold the key only. A filter on key columns applies as it is.
 * A comparison of a column with a monotonic time function of it in the key applies to the function's value. Other
 * filters apply in the tasks only.
 */
private[primaryindex] object IndexConditions {

  def apply(key: Seq[KeyEntry], filter: PushedFilter): Seq[String] = filter match {
    case PushedFilter.Other(sql, columns) =>
      val entries = key.collect { case entry: ColumnEntry if columns.contains(entry.name) => entry }
      if (columns.subsetOf(entries.map(_.name).toSet)) Seq(orUnloaded(key, entries, sql)) else Nil
    case _ => key.flatMap(entry => entry.indexCondition(filter).map(orUnloaded(key, Seq(entry), _)))
  }

  // key entries after a high-cardinality first entry may be left unloaded, reading as their default: keep those
  private def orUnloaded(key: Seq[KeyEntry], entries: Seq[KeyEntry], condition: String): String = {
    val unloaded = entries.filterNot(_ == key.head).map(_.identifier).distinct.sorted
      .map(id => s"$id = defaultValueOfArgumentType($id)")
    (s"($condition)" +: unloaded).mkString(" OR ")
  }
}
