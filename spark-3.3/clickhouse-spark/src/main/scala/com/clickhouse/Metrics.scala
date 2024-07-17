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

package com.clickhouse

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import com.clickhouse.Metrics._

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
