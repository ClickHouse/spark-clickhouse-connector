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

/** Groups the ClickHouse partitions of a table into a bounded number of read tasks. */
object PartitionGroups {

  /**
   * Spreads `partitions` over at most `tasks` groups holding about as many rows each: the largest partition first,
   * each into the group with the fewest rows so far. Fewer partitions than tasks give one group each.
   */
  def group(partitions: Seq[PartitionSpec], tasks: Int): Seq[Seq[PartitionSpec]] = {
    require(tasks > 0, s"Not a positive number of tasks: $tasks")
    val groups = Array.fill(math.min(tasks, partitions.size))(Vector.empty[Int])
    val rows = Array.fill(groups.length)(0L)
    partitions.indices.sortBy(i => -partitions(i).row_count).foreach { i =>
      val fewest = rows.indices.minBy(g => (rows(g), groups(g).size))
      groups(fewest) :+= i
      rows(fewest) += partitions(i).row_count
    }
    groups.toSeq.map(_.sorted.map(partitions))
  }

  /** The rows of a group of partitions as a WHERE conjunct: by their ids, or by the values of `partitionKey`. */
  def filterExpr(group: Seq[PartitionSpec], partitionKey: String, byId: Boolean): String =
    if (group == Seq(NoPartitionSpec)) "1=1"
    else if (byId) group.map(p => s"'${p.partition_id}'").mkString("_partition_id IN (", ", ", ")")
    else group.map(p => compileValue(p.partition_value)).mkString(s"$partitionKey IN (", ", ", ")")

  // TODO improve and test
  private def compileValue(partitionValue: String): String =
    (partitionValue.contains("-"), partitionValue.contains("(")) match {
      // quote when partition by a single Date Type column to avoid illegal types of arguments (Date, Int64)
      case (true, false) => s"'$partitionValue'"
      // Date type column is quoted if there are multi partition columns
      case _ => s"$partitionValue"
    }
}
