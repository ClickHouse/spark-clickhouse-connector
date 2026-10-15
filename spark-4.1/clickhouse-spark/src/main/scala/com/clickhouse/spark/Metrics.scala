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

package com.clickhouse.spark

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import Metrics._

case class TaskMetric(override val name: String, override val value: Long) extends CustomTaskMetric

abstract class SizeSumMetric extends CustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = Utils.bytesToString(taskMetrics.sum)
}

abstract class DurationSumMetric extends CustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = Utils.msDurationToString(taskMetrics.sum)
}

object Metrics {
  val BLOCKS_READ = "blocksRead"
  val BYTES_READ = "bytesRead"

  val RECORDS_WRITTEN = "recordsWritten"
  val BYTES_WRITTEN = "bytesWritten"
  val SERIALIZE_TIME = "serializeTime"
  val WRITE_TIME = "writeTime"
  val FLUSHES = "flushes"
  val FAILED_WRITE_ATTEMPTS = "failedWriteAttempts"
  val MIN_BATCH_SIZE = "minBatchSize"
  val MAX_BATCH_SIZE = "maxBatchSize"
  val BATCH_FILL_0_25 = "batchFill0to25"
  val BATCH_FILL_25_50 = "batchFill25to50"
  val BATCH_FILL_50_75 = "batchFill50to75"
  val BATCH_FILL_75_100 = "batchFill75to100"
  val CLIENTS = "clients"
}

case class BlocksReadMetric() extends CustomSumMetric {
  override def name: String = BLOCKS_READ
  override def description: String = "number of blocks"
}

case class BytesReadMetric() extends SizeSumMetric {
  override def name: String = BYTES_READ
  override def description: String = "data size"
}

case class RecordsWrittenMetric() extends CustomSumMetric {
  override def name: String = RECORDS_WRITTEN
  override def description: String = "number of output rows"
}

case class BytesWrittenMetric() extends SizeSumMetric {
  override def name: String = BYTES_WRITTEN
  override def description: String = "written output"
}

case class SerializeTimeMetric() extends DurationSumMetric {
  override def name: String = SERIALIZE_TIME
  override def description: String = "total time of serialization"
}

case class WriteTimeMetric() extends DurationSumMetric {
  override def name: String = WRITE_TIME
  override def description: String = "total time of writing"
}

case class FlushCountMetric() extends CustomSumMetric {
  override def name: String = FLUSHES
  override def description: String = "total batch writes to ClickHouse"
}

case class FailedWriteAttemptsMetric() extends CustomSumMetric {
  override def name: String = FAILED_WRITE_ATTEMPTS
  override def description: String = "failed write attempts to ClickHouse"
}

case class MinBatchSizeMetric() extends CustomMetric {
  override def name: String = MIN_BATCH_SIZE
  override def description: String = "min batch size written (rows)"
  // a task that flushed nothing reports 0; ignore it
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String =
    taskMetrics.filter(_ > 0).reduceOption(_ min _).getOrElse(0L).toString
}

case class MaxBatchSizeMetric() extends CustomMetric {
  override def name: String = MAX_BATCH_SIZE
  override def description: String = "max batch size written (rows)"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String =
    taskMetrics.reduceOption(_ max _).getOrElse(0L).toString
}

case class BatchFill0To25Metric() extends CustomSumMetric {
  override def name: String = BATCH_FILL_0_25
  override def description: String = "batches 0-25% of configured batch size"
}

case class BatchFill25To50Metric() extends CustomSumMetric {
  override def name: String = BATCH_FILL_25_50
  override def description: String = "batches 25-50% of configured batch size"
}

case class BatchFill50To75Metric() extends CustomSumMetric {
  override def name: String = BATCH_FILL_50_75
  override def description: String = "batches 50-75% of configured batch size"
}

case class BatchFill75To100Metric() extends CustomSumMetric {
  override def name: String = BATCH_FILL_75_100
  override def description: String = "batches 75-100% of configured batch size"
}

case class ClientsMetric() extends CustomSumMetric {
  override def name: String = CLIENTS
  override def description: String = "clients connected to ClickHouse"
}
