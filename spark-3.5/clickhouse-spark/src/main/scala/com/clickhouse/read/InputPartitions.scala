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

package com.clickhouse.read

import com.clickhouse.spec.{NoPartitionSpec, NodeSpec, Nodes, PartitionSpec, TableSpec}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import com.clickhouse.spec._

case class ClickHousePartitioning(inputParts: Array[ClickHouseInputPartition]) extends Partitioning {

  override def numPartitions(): Int = inputParts.length

}

case class ClickHouseInputPartition(
  table: TableSpec,
  partition: PartitionSpec,
  filterByPartitionId: Boolean,
  candidateNodes: Nodes, // try to use them only when preferredNode unavailable
  preferredNode: Option[NodeSpec] = None // TODO assigned by ScanBuilder in Spark Driver side
) extends InputPartition {

  override def preferredLocations(): Array[String] = preferredNode match {
    case Some(preferred) => Array(preferred.host)
    case None => candidateNodes.nodes.map(_.host)
  }

  def partFilterExpr: String = partition match {
    case NoPartitionSpec => "1=1"
    case PartitionSpec(_, partitionId, _, _) if filterByPartitionId =>
      s"_partition_id = '$partitionId'"
    case PartitionSpec(partitionValue, _, _, _) =>
      s"${table.partition_key} = ${compilePartitionFilterValue(partitionValue)}"
  }

  // TODO improve and test
  def compilePartitionFilterValue(partitionValue: String): String =
    (partitionValue.contains("-"), partitionValue.contains("(")) match {
      // quote when partition by a single Date Type column to avoid illegal types of arguments (Date, Int64)
      case (true, false) => s"'$partitionValue'"
      // Date type column is quoted if there are multi partition columns
      case _ => s"$partitionValue"
    }
}
