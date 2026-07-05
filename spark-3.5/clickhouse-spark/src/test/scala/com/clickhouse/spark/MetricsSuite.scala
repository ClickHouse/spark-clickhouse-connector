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
import com.clickhouse.spark.func.StaticFunctionRegistry
import com.clickhouse.spark.spec.{MergeTreeEngineSpec, NodeSpec, TableSpec}
import com.clickhouse.spark.write.{ClickHouseWrite, ClickHouseWriter, WriteJobDescription}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.WriteOptions
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, ZoneId}
import java.util.Collections

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
    val supported = new ClickHouseWrite(writeJob).supportedCustomMetrics().map(_.name).toSet
    assert(new StubWriter(writeJob).currentMetricsValues.map(_.name).toSet === supported)
  }

  test("fresh writer reports zero metrics without opening a connection") {
    val metrics = metricsMap(new StubWriter(writeJob))
    assert(metrics(RECORDS_WRITTEN) === 0L)
    assert(metrics(FLUSHES) === 0L)
    assert(metrics(MIN_BATCH_SIZE) === 0L)
    assert(metrics(MAX_BATCH_SIZE) === 0L)
    assert(metrics(CONNECTIONS) === 0L)
  }

  test("buffered rows count as one pending flush") {
    val writer = new StubWriter(writeJob)
    (1 to 3).foreach(_ => writer.write(InternalRow.empty))
    val metrics = metricsMap(writer)
    assert(metrics(RECORDS_WRITTEN) === 3L)
    assert(metrics(FLUSHES) === 1L)
    assert(metrics(MIN_BATCH_SIZE) === 3L)
    assert(metrics(MAX_BATCH_SIZE) === 3L)
    assert(metrics(CONNECTIONS) === 1L)
  }

  test("pending flush combines with flushed batches") {
    val writer = new StubWriter(writeJob)
    writer._flushes.add(2)
    writer._minBatchSize = 100L
    writer._maxBatchSize = 250L
    writer._totalRecordsWritten.add(350L)
    (1 to 5).foreach(_ => writer.write(InternalRow.empty))
    val metrics = metricsMap(writer)
    assert(metrics(RECORDS_WRITTEN) === 355L)
    assert(metrics(FLUSHES) === 3L)
    assert(metrics(MIN_BATCH_SIZE) === 5L)
    assert(metrics(MAX_BATCH_SIZE) === 250L)
    assert(metrics(CONNECTIONS) === 1L)
  }

  private def metricsMap(writer: ClickHouseWriter): Map[String, Long] =
    writer.currentMetricsValues.map(m => m.name -> m.value).toMap

  private class StubWriter(job: WriteJobDescription) extends ClickHouseWriter(job) {
    override protected lazy val client: Either[ClusterClient, NodeClient] = Right(null)
    override def format: String = "Stub"
    override def writeRow(record: InternalRow): Unit = ()
    override def doSerialize(): Array[Byte] = Array.emptyByteArray
  }

  private def writeJob: WriteJobDescription = WriteJobDescription(
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
    writeOptions = new WriteOptions(Collections.emptyMap[String, String]),
    writeSettings = Map.empty,
    functionRegistry = StaticFunctionRegistry
  )
}
