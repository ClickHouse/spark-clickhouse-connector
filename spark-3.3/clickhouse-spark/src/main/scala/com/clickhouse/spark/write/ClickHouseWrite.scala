/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under th e License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.spark.write

import com.clickhouse.spark.{BytesWrittenMetric, RecordsWrittenMetric, SerializeTimeMetric, WriteTimeMetric}
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.write.format.{ClickHouseArrowStreamWriter, ClickHouseJsonEachRowWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter
import com.clickhouse.spark._
import com.clickhouse.spark.spec.DistributedEngineSpec
import org.apache.spark.sql.sources.AlwaysTrue

class ClickHouseWriteBuilder(writeJob: WriteJobDescription)
    extends WriteBuilder
    with SupportsOverwrite
    with Logging {

  private var isOverwrite: Boolean = false
  private var overwriteFilters: Array[Filter] = Array.empty

  override def build(): Write = new ClickHouseWrite(writeJob, isOverwrite, overwriteFilters)

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    log.info(s"Overwrite mode for table ${writeJob.targetDatabase(false)}.${writeJob.targetTable(false)}")
    isOverwrite = true
    overwriteFilters = filters

    // Check if we have actual partition filters (not AlwaysTrue or empty)
    val hasPartitionFilters = filters.nonEmpty && !filters.forall {
      case _: AlwaysTrue => true
      case _ => false
    }
    this
  }
}

class ClickHouseWrite(
  writeJob: WriteJobDescription,
  isOverwrite: Boolean = false,
  overwriteFilters: Array[Filter] = Array.empty
) extends Write
    with RequiresDistributionAndOrdering
    with SQLConfHelper
    with Logging {

  override def description: String =
    s"ClickHouseWrite(database=${writeJob.targetDatabase(false)}, table=${writeJob.targetTable(false)})})"

  override def requiredDistribution(): Distribution = Distributions.clustered(writeJob.sparkSplits.toArray)

  override def requiredNumPartitions(): Int = conf.getConf(WRITE_REPARTITION_NUM)

  override def requiredOrdering(): Array[SortOrder] = writeJob.sparkSortOrders

  override def toBatch: BatchWrite = new ClickHouseBatchWrite(writeJob, isOverwrite)

  override def supportedCustomMetrics(): Array[CustomMetric] = Array(
    RecordsWrittenMetric(),
    BytesWrittenMetric(),
    SerializeTimeMetric(),
    WriteTimeMetric()
  )
}

class ClickHouseBatchWrite(
  writeJob: WriteJobDescription,
  isOverwrite: Boolean = false
) extends BatchWrite with DataWriterFactory with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    // Truncate table before writing if overwrite mode is enabled
    if (isOverwrite) {
      truncateTable()
    }
    this
  }

  private def truncateTable(): Unit = {
    import com.clickhouse.spark.client.NodeClient
    import com.clickhouse.spark.Utils

    log.info(s"Truncating table ${writeJob.targetDatabase(false)}.${writeJob.targetTable(false)} for overwrite mode")

    Utils.tryWithResource(NodeClient(writeJob.node)) { implicit nodeClient =>
      writeJob.tableEngineSpec match {
        case DistributedEngineSpec(_, cluster, local_db, local_table, _, _) =>
          val sql = s"TRUNCATE TABLE IF EXISTS `$local_db`.`$local_table` ON CLUSTER `$cluster`"
          nodeClient.syncQueryAndCheckOutputJSONEachRow(sql)
        case _ =>
          val sql = s"TRUNCATE TABLE IF EXISTS `${writeJob.targetDatabase(false)}`.`${writeJob.targetTable(false)}`"
          nodeClient.syncQueryAndCheckOutputJSONEachRow(sql)
      }
    }
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val format = writeJob.writeOptions.format
    format match {
      case "json" => new ClickHouseJsonEachRowWriter(writeJob)
      case "arrow" => new ClickHouseArrowStreamWriter(writeJob)
      case unsupported => throw CHClientException(s"Unsupported write format: $unsupported")
    }
  }
}
