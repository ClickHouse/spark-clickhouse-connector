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

package xenon.clickhouse.read

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.partitioning.{Distribution, Partitioning}
import xenon.clickhouse.spec._

case class ClickHousePartitioning(inputParts: Array[ClickHouseInputPartition]) extends Partitioning {

  override def numPartitions(): Int = inputParts.length

  override def satisfy(distribution: Distribution): Boolean = false
}

case class ClickHouseInputPartition(
  table: TableSpec,
  partition: PartitionSpec,
  candidateNodes: Nodes, // try to use them only when preferredNode unavailable
  preferredNode: Option[NodeSpec] = None // TODO assigned by ScanBuilder in Spark Driver side
) extends InputPartition {

  override def preferredLocations(): Array[String] = preferredNode match {
    case Some(preferred) => Array(preferred.host)
    case None => candidateNodes.nodes.map(_.host)
  }

  def partFilterExpr: String = partition match {
    case NoPartitionSpec => "1=1"
    case PartitionSpec(part, _, _) => s"${table.partition_key} = $part"
  }
}
