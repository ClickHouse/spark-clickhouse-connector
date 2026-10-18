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

package com.clickhouse.spark.write.coalesce

import com.clickhouse.spark.Metrics.{BYTES_WRITTEN, RECORDS_WRITTEN}
import com.clickhouse.spark.func.StaticFunctionRegistry
import com.clickhouse.spark.spec.{MergeTreeEngineSpec, NodeSpec, TableSpec}
import com.clickhouse.spark.write.{ClickHouseWriter, WriteJobDescription}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.WriteOptions
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.atomic.{AtomicLong, LongAdder}
import java.util.{HashMap => JHashMap}

/**
 * Regression guard for the coalesce-write output-metrics bug: with
 * `spark.clickhouse.write.coalesce.enabled=true`, the rows/bytes a task handed off to the insert
 * coordinator used to be credited only in commit(), so Spark's final pre-commit sample of
 * currentMetricsValues read RECORDS_WRITTEN = 0 and BYTES_WRITTEN = 0.
 *
 * This suite is fully server-free: it drives a stub ClickHouseWriter whose record-batch
 * serialization and node client are replaced, and swaps the coordinator's sender for a fake one.
 */
class CoalesceWriterMetricsSuite extends AnyFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = InsertCoordinator.shutdownForTest()

  private val batchSize = 100
  private val batchPayloadSize = 11 // fixed, distinct from the row count, so bytes are tracked on their own

  private val schema = StructType(Seq(StructField("id", IntegerType, nullable = false)))

  private def writeOptions: WriteOptions = {
    val m = new JHashMap[String, String]()
    m.put("spark.clickhouse.write.format", "arrow")
    m.put("spark.clickhouse.write.coalesce.enabled", "true")
    m.put("spark.clickhouse.write.batchSize", batchSize.toString)
    // Large target + no linger: the coordinator buffers every batch and only drains on the
    // flushNow() that commit() issues, so the pre-commit metric is exercised before any send.
    m.put("spark.clickhouse.write.coalesce.targetBytes", "1g")
    m.put("spark.clickhouse.write.coalesce.lingerMs", "0")
    m.put("spark.clickhouse.write.coalesce.senderThreads", "1")
    m.put("spark.clickhouse.write.dedup.mode", "none")
    new WriteOptions(m)
  }

  private def tableSpec: TableSpec = TableSpec(
    database = "db",
    name = "t",
    uuid = "",
    engine = "MergeTree",
    is_temporary = false,
    data_paths = Nil,
    metadata_path = "",
    metadata_modification_time = LocalDateTime.of(2020, 1, 1, 0, 0),
    dependencies_database = Nil,
    dependencies_table = Nil,
    create_table_query = "",
    engine_full = "",
    partition_key = "",
    sorting_key = "",
    primary_key = "",
    sampling_key = "",
    storage_policy = "",
    total_rows = None,
    total_bytes = None,
    lifetime_rows = None,
    lifetime_bytes = None
  )

  private def writeJob: WriteJobDescription = WriteJobDescription(
    queryId = "q",
    tableSchema = schema,
    metadataSchema = StructType(Nil),
    dataSetSchema = schema,
    node = NodeSpec("localhost"),
    tz = ZoneId.of("UTC"),
    tableSpec = tableSpec,
    tableEngineSpec = MergeTreeEngineSpec("MergeTree"),
    cluster = None,
    localTableSpec = None,
    localTableEngineSpec = None,
    shardingKey = None,
    partitionKey = None,
    sortingKey = None,
    writeOptions = writeOptions,
    functionRegistry = StaticFunctionRegistry
  )

  /**
   * Stub writer that keeps all the real coalesce accounting (submit -> currentMetricsValues ->
   * commit) but replaces the Arrow serialization with a fixed-size payload and the node client with
   * a no-op, so no ClickHouse server or Spark Arrow allocator is needed.
   */
  private class StubCoalesceWriter extends ClickHouseWriter(writeJob) {
    override def format: String = "ArrowStream"
    override def coalesceSupported: Boolean = true
    override def writeRow(record: InternalRow): Unit = () // base write() counts the row for us
    override def doSerialize(): Array[Byte] = Array.emptyByteArray
    override def schemaHeaderBytes: Array[Byte] = Array.emptyByteArray
    override def serializeRecordBatch(): BatchPayload = new BytesPayload(new Array[Byte](batchPayloadSize))
    // Never touch the real client: the fake sender ignores ctx.client anyway.
    override def nodeClient(shardNum: Option[Int]): com.clickhouse.spark.client.NodeClient = null
  }

  private def metric(w: ClickHouseWriter, name: String): Long =
    w.currentMetricsValues.find(_.name == name).map(_.value)
      .getOrElse(throw new AssertionError(s"no metric named $name"))

  test("coalesce mode reports handed-off rows and bytes before commit, and credits them once at commit") {
    val sentRows = new LongAdder
    val sentBytes = new LongAdder
    val sentGroups = new AtomicLong(0)
    InsertCoordinator.setSenderForTest { (_, group) =>
      sentGroups.incrementAndGet()
      group.foreach { h =>
        sentRows.add(h.rows)
        sentBytes.add(h.payload.sizeBytes)
        h.payload.close()
        h.promise.success(())
      }
    }

    val writer = new StubCoalesceWriter
    val rows = 3 * batchSize // 300 -> exactly 3 coalesced flushes, nothing left buffered

    // Before writing anything: nothing handed off yet.
    assert(metric(writer, RECORDS_WRITTEN) == 0L)
    assert(metric(writer, BYTES_WRITTEN) == 0L)

    (1 to rows).foreach(_ => writer.write(null))

    // BEFORE commit: the regression. The coordinator has NOT drained yet (large target, no linger),
    // so the fake sender has not run, yet Spark's sample must already see the handed-off work.
    assert(sentGroups.get() == 0L, "precondition: no group should have been sent before commit")
    assert(metric(writer, RECORDS_WRITTEN) == rows.toLong, "pre-commit RECORDS_WRITTEN must reflect handed-off rows")
    assert(
      metric(writer, BYTES_WRITTEN) == 3L * batchPayloadSize,
      "pre-commit BYTES_WRITTEN must reflect handed-off serialized bytes"
    )

    writer.commit()

    // The commit flush drained everything through the (fake) sender exactly once.
    assert(sentRows.sum() == rows.toLong)
    assert(sentBytes.sum() == 3L * batchPayloadSize)

    // AFTER commit: the figure is unchanged (credit moved into the totals, in-flight tallies zeroed)
    // -> no double counting.
    assert(metric(writer, RECORDS_WRITTEN) == rows.toLong, "post-commit RECORDS_WRITTEN must not double count")
    assert(metric(writer, BYTES_WRITTEN) == 3L * batchPayloadSize, "post-commit BYTES_WRITTEN must not double count")

    // The reported figures equal what the sender actually received (rows/bytes handed off).
    assert(metric(writer, RECORDS_WRITTEN) == sentRows.sum())
    assert(metric(writer, BYTES_WRITTEN) == sentBytes.sum())

    writer.close()
  }
}
