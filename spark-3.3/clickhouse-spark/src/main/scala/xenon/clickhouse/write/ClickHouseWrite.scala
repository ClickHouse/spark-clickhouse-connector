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

package xenon.clickhouse.write

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write._
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.write.format.{ClickHouseArrowStreamWriter, ClickHouseJsonEachRowWriter}

class ClickHouseWriteBuilder(writeJob: WriteJobDescription) extends WriteBuilder {

  override def build(): Write = new ClickHouseWrite(writeJob)
}

class ClickHouseWrite(
  writeJob: WriteJobDescription
) extends Write
    with RequiresDistributionAndOrdering
    with SQLConfHelper {

  // for SPARK-37523
  def distributionStrictlyRequired: Boolean = writeJob.writeOptions.repartitionStrictly

  override def requiredDistribution(): Distribution = Distributions.clustered(writeJob.sparkSplits.toArray)

  override def requiredNumPartitions(): Int = conf.getConf(WRITE_REPARTITION_NUM)

  override def requiredOrdering(): Array[SortOrder] = writeJob.sparkSortOrders

  override def toBatch: BatchWrite = new ClickHouseBatchWrite(writeJob)
}

class ClickHouseBatchWrite(
  writeJob: WriteJobDescription
) extends BatchWrite with DataWriterFactory {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = this

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    writeJob.writeOptions.format match {
      case "JSONEachRow" => new ClickHouseJsonEachRowWriter(writeJob)
      case "ArrowStream" => new ClickHouseArrowStreamWriter(writeJob)
      case unsupported => throw ClickHouseClientException(s"Unsupported write format: $unsupported")
    }
}
