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

import com.clickhouse.spark.Metrics._
import com.clickhouse.spark.client.{ClusterClient, NodeClient}
import com.clickhouse.spark.exception.{CHException, CHServerException, RetryableCHException}
import com.clickhouse.spark.format.SimpleOutput
import com.clickhouse.spark.spec.{MergeTreeEngineSpec, NodeSpec, TableSpec}
import com.clickhouse.spark.write.{ClickHouseWrite, ClickHouseWriter, WriteJobDescription}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.WriteOptions
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import java.io.InputStream
import java.time.{LocalDateTime, ZoneId}

class MetricsSuite extends AnyFunSuite {

  test("minBatchSize aggregation ignores tasks that flushed nothing") {
    assert(MinBatchSizeMetric().aggregateTaskMetrics(Array(300L, 0L, 100L)) === "100")
  }

  test("minBatchSize aggregation reports 0 when no task flushed") {
    assert(MinBatchSizeMetric().aggregateTaskMetrics(Array.empty[Long]) === "0")
    assert(MinBatchSizeMetric().aggregateTaskMetrics(Array(0L, 0L)) === "0")
  }

  test("maxBatchSize aggregation") {
    assert(MaxBatchSizeMetric().aggregateTaskMetrics(Array(300L, 0L, 100L)) === "300")
    assert(MaxBatchSizeMetric().aggregateTaskMetrics(Array.empty[Long]) === "0")
  }

  test("writer reports a task metric for every supported custom metric") {
    val supported = new ClickHouseWrite(writeJob()).supportedCustomMetrics().map(_.name).toSet
    assert(new StubWriter(writeJob()).currentMetricsValues.map(_.name).toSet === supported)
  }

  test("buffered rows count as one pending flush") {
    val writer = new StubWriter(writeJob())
    (1 to 3).foreach(_ => writer.write(InternalRow.empty))
    val metrics = metricsMap(writer)
    assert(metrics(RECORDS_WRITTEN) === 3L)
    assert(metrics(FLUSHES) === 1L)
    assert(metrics(FAILED_WRITE_ATTEMPTS) === 0L)
    assert(metrics(MIN_BATCH_SIZE) === 3L)
    assert(metrics(MAX_BATCH_SIZE) === 3L)
    assert(metrics(BATCH_FILL_0_25) === 1L) // 3 pending rows of the default 10k batchSize
    assert(metrics(BATCH_FILL_75_100) === 0L)
    assert(metrics(CONNECTIONS) === 1L)
  }

  test("pending flush combines with flushed batches") {
    val writer = new StubWriter(writeJob())
    writer._flushes.add(2)
    writer._minBatchSize = 100L
    writer._maxBatchSize = 250L
    writer._batchFillBuckets(3).add(2)
    writer._totalRecordsWritten.add(350L)
    (1 to 5).foreach(_ => writer.write(InternalRow.empty))
    val metrics = metricsMap(writer)
    assert(metrics(RECORDS_WRITTEN) === 355L)
    assert(metrics(FLUSHES) === 3L)
    assert(metrics(MIN_BATCH_SIZE) === 5L)
    assert(metrics(MAX_BATCH_SIZE) === 250L)
    assert(metrics(BATCH_FILL_0_25) === 1L) // the pending batch
    assert(metrics(BATCH_FILL_25_50) === 0L)
    assert(metrics(BATCH_FILL_75_100) === 2L) // the seeded full batches
    assert(metrics(CONNECTIONS) === 1L)
  }

  test("non-retryable write failure counts one failed attempt") {
    val job = writeJob("spark.clickhouse.write.retryableErrorCodes" -> "241")
    val writer = new StubWriter(job, new FailingNodeClient(errorCode = 999))
    writer.write(InternalRow.empty)
    intercept[CHServerException](writer.doFlush(None))
    assert(metricsMap(writer)(FAILED_WRITE_ATTEMPTS) === 1L)
  }

  test("every retried write attempt counts as a failed attempt") {
    val job = writeJob(
      "spark.clickhouse.write.retryableErrorCodes" -> "999",
      "spark.clickhouse.write.maxRetry" -> "1",
      "spark.clickhouse.write.retryInterval" -> "0"
    )
    val writer = new StubWriter(job, new FailingNodeClient(errorCode = 999))
    writer.write(InternalRow.empty)
    intercept[RetryableCHException](writer.doFlush(None))
    assert(metricsMap(writer)(FAILED_WRITE_ATTEMPTS) === 2L)
  }

  private def metricsMap(writer: ClickHouseWriter): Map[String, Long] =
    writer.currentMetricsValues.map(m => m.name -> m.value).toMap

  private class StubWriter(job: WriteJobDescription, stubClient: NodeClient = null)
      extends ClickHouseWriter(job) {
    override protected lazy val client: Either[ClusterClient, NodeClient] = Right(stubClient)
    override def format: String = "Stub"
    override def writeRow(record: InternalRow): Unit = ()
    override def doSerialize(): Array[Byte] = Array.emptyByteArray
  }

  private class FailingNodeClient(errorCode: Int) extends NodeClient(NodeSpec("127.0.0.1", Some(8123))) {
    override def syncInsertOutputJSONEachRow(
      database: String,
      table: String,
      inputFormat: String,
      data: InputStream,
      settings: Map[String, String]
    ): Either[CHException, SimpleOutput[ObjectNode]] =
      Left(CHServerException(errorCode, "simulated failure", None, None))
  }

  private def writeJob(options: (String, String)*): WriteJobDescription = {
    val optionsMap = new java.util.HashMap[String, String]()
    options.foreach { case (k, v) => optionsMap.put(k, v) }
    WriteJobDescription(
      queryId = "metrics-suite-query",
      tableSchema = new StructType(),
      metadataSchema = new StructType(),
      dataSetSchema = new StructType(),
      node = NodeSpec("127.0.0.1"),
      tz = ZoneId.of("UTC"),
      tableSpec = TableSpec(
        database = "db",
        name = "tbl",
        uuid = "",
        engine = "MergeTree",
        is_temporary = false,
        data_paths = Nil,
        metadata_path = "",
        metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
        dependencies_database = Nil,
        dependencies_table = Nil,
        create_table_query = "",
        engine_full = "MergeTree",
        partition_key = "",
        sorting_key = "",
        primary_key = "",
        sampling_key = "",
        storage_policy = "",
        total_rows = None,
        total_bytes = None,
        lifetime_rows = None,
        lifetime_bytes = None
      ),
      tableEngineSpec = MergeTreeEngineSpec("MergeTree()"),
      cluster = None,
      localTableSpec = None,
      localTableEngineSpec = None,
      shardingKey = None,
      partitionKey = None,
      sortingKey = None,
      writeOptions = new WriteOptions(optionsMap),
      writeSettings = Map.empty
    )
  }
}
