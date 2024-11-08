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

import com.clickhouse.client.ClickHouseResponse
import com.clickhouse.data.ClickHouseCompression
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import com.clickhouse.spark.Metrics.{BLOCKS_READ, BYTES_READ}
import com.clickhouse.spark.client.{NodeClient, NodesClient}
import com.clickhouse.spark.format.StreamOutput
import com.clickhouse.spark.{ClickHouseHelper, Logging, TaskMetric}

abstract class ClickHouseReader[Record](
  scanJob: ScanJobDescription,
  part: ClickHouseInputPartition
) extends PartitionReader[InternalRow]
    with ClickHouseHelper
    with SQLConfHelper
    with Logging {

  val readDistributedUseClusterNodes: Boolean = conf.getConf(READ_DISTRIBUTED_USE_CLUSTER_NODES)
  val readDistributedConvertLocal: Boolean = conf.getConf(READ_DISTRIBUTED_CONVERT_LOCAL)
  private val readWithSettings: String = conf.getConf(READ_WITH_SETTINGS)

  val database: String = part.table.database
  val table: String = part.table.name
  val codec: ClickHouseCompression = scanJob.readOptions.compressionCodec
  val readSchema: StructType = scanJob.readSchema

  private lazy val nodesClient = NodesClient(part.candidateNodes)

  def nodeClient: NodeClient = nodesClient.node

  lazy val scanQuery: String = {
    val selectItems =
      if (readSchema.isEmpty) {
        "1" // for case like COUNT(*) which prunes all columns
      } else {
        readSchema.map {
          field => if (scanJob.groupByClause.isDefined) field.name else s"`${field.name}`"
        }.mkString(", ")
      }
    s"""SELECT $selectItems
       |FROM `$database`.`$table`
       |WHERE (${part.partFilterExpr}) AND (${scanJob.filtersExpr})
       |${scanJob.groupByClause.getOrElse("")}
       |${scanJob.limit.map(n => s"LIMIT $n").getOrElse("")}
       |${if (readWithSettings.nonEmpty) s"SETTINGS $readWithSettings" else ""}
       |""".stripMargin
  }

  def format: String

  lazy val resp: ClickHouseResponse = nodeClient.queryAndCheck(scanQuery, format, codec)

  def totalBlocksRead: Long = resp.getSummary.getStatistics.getBlocks

  def totalBytesRead: Long = resp.getSummary.getReadBytes

  override def currentMetricsValues: Array[CustomTaskMetric] = Array(
    TaskMetric(BLOCKS_READ, totalBlocksRead),
    TaskMetric(BYTES_READ, totalBytesRead)
  )

  def streamOutput: Iterator[Record]

  private var currentRecord: Record = _

  override def next(): Boolean = {
    val hasNext = streamOutput.hasNext
    if (hasNext) currentRecord = streamOutput.next
    hasNext
  }

  override def get: InternalRow = decode(currentRecord)

  def decode(record: Record): InternalRow

  override def close(): Unit = nodesClient.close()
}
