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

/** A ClickHouse partition of `table` read in one task, from one of `nodes`. */
case class TablePartition(table: TableSpec, nodes: Nodes, partition: PartitionSpec) {

  /** The rows of the partition as a WHERE conjunct: by the partition id, or by the value of the partition key. */
  def filterExpr(byId: Boolean): String = partition match {
    case NoPartitionSpec => "1=1"
    case PartitionSpec(_, partitionId, _, _) if byId => s"_partition_id = '$partitionId'"
    case PartitionSpec(partitionValue, _, _, _) => s"${table.partition_key} = ${compileValue(partitionValue)}"
  }

  // TODO improve and test
  private def compileValue(partitionValue: String): String =
    (partitionValue.contains("-"), partitionValue.contains("(")) match {
      // quote when partition by a single Date Type column to avoid illegal types of arguments (Date, Int64)
      case (true, false) => s"'$partitionValue'"
      // Date type column is quoted if there are multi partition columns
      case _ => s"$partitionValue"
    }
}
