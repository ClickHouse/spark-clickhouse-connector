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

// under `com.clickhouse.spark` (unlike the sibling suites) to reach the `private[spark]` analytics classes
package com.clickhouse.spark.single

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn}
import com.clickhouse.spark.read.{ClickHouseBatchScan, ScanJobDescription}
import com.clickhouse.spark.write.{ClickHouseBatchWrite, WriteJobDescription}
import com.clickhouse.spark.telemetry.UserAnalyticsFactory
import com.clickhouse.spark.{ClickHouseCatalog, ClickHouseTable}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.SEND_ANONYMOUS_USAGE_STATS
import org.apache.spark.sql.clickhouse.single.SparkClickHouseSingleTest
import org.apache.spark.sql.clickhouse.{ReadOptions, WriteOptions}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.tags.Cloud

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

@Cloud
class ClickHouseCloudUserAnalyticsEventsSuite extends UserAnalyticsEventsSuite with ClickHouseCloudMixIn

class ClickHouseSingleUserAnalyticsEventsSuite extends UserAnalyticsEventsSuite with ClickHouseSingleMixIn

abstract class UserAnalyticsEventsSuite extends SparkClickHouseSingleTest {

  // independent re-implementations, so the assertions do not reuse the production derivations
  private def expectedSha256Hex16(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(b => f"$b%02x").mkString.take(16)

  private def expectedTableId(uuid: String): String = uuid.toLowerCase.replace("-", "").take(16)

  /** Loads the table through the production catalog path: `initialize` + `loadTable`. */
  private def loadClickHouseTable(db: String, tbl: String): ClickHouseTable = {
    val catalogOptions = new java.util.HashMap[String, String]()
    cmdRunnerOptions.foreach { case (key, value) => catalogOptions.put(key, value) }
    val catalog = new ClickHouseCatalog()
    catalog.initialize("clickhouse", new CaseInsensitiveStringMap(catalogOptions))
    catalog.loadTable(Identifier.of(Array(db), tbl)).asInstanceOf[ClickHouseTable]
  }

  /** Ground truth fetched from the live server, independently of the catalog metadata. */
  private def tableUuidAndEngine(db: String, tbl: String): (String, String) = {
    var uuidAndEngine: (String, String) = null
    withNodeClient() { client =>
      val row = client.syncQueryAndCheckOutputJSONEachRow(
        s"SELECT uuid, engine FROM system.tables WHERE database = '$db' AND name = '$tbl'"
      ).records.head
      uuidAndEngine = (row.get("uuid").asText, row.get("engine").asText)
    }
    uuidAndEngine
  }

  private def scanJob(table: ClickHouseTable, readOptions: ReadOptions): ScanJobDescription =
    ScanJobDescription(
      node = table.node,
      tz = table.tz,
      tableSpec = table.spec,
      tableEngineSpec = table.engineSpec,
      cluster = table.cluster,
      localTableSpec = table.localTableSpec,
      localTableEngineSpec = table.localTableEngineSpec,
      readOptions = readOptions,
      functionRegistry = table.functionRegistry
    )

  private def writeJob(table: ClickHouseTable, writeOptions: WriteOptions): WriteJobDescription =
    WriteJobDescription(
      queryId = UUID.randomUUID().toString,
      tableSchema = table.schema,
      metadataSchema = new StructType(),
      dataSetSchema = table.schema,
      node = table.node,
      tz = table.tz,
      tableSpec = table.spec,
      tableEngineSpec = table.engineSpec,
      cluster = table.cluster,
      localTableSpec = table.localTableSpec,
      localTableEngineSpec = table.localTableEngineSpec,
      shardingKey = table.shardingKey,
      partitionKey = table.partitionKey,
      sortingKey = table.sortingKey,
      writeOptions = writeOptions,
      writeSettings = Map.empty,
      functionRegistry = table.functionRegistry
    )

  private def options(entries: (String, String)*): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    entries.foreach { case (key, value) => map.put(key, value) }
    map
  }

  private val onePartition: PhysicalWriteInfo = new PhysicalWriteInfo {
    override def numPartitions(): Int = 1
  }

  test("the production read and write hooks report one event carrying the job's metadata") {
    withSimpleTable("usage_stats_db", "usage_stats_tbl", writeData = true) { (db, tbl) =>
      spark.sql(s"SELECT * FROM `$db`.`$tbl`").collect() // a real job: the analytics-wired plan serializes and runs

      val table = loadClickHouseTable(db, tbl)
      val (uuid, engine) = tableUuidAndEngine(db, tbl)
      assert(table.spec.uuid === uuid)

      val enabled = options(SEND_ANONYMOUS_USAGE_STATS.key -> "true") // the shared IT conf turns reporting off
      val readCapture = UserAnalyticsFactory.createCapture()
      val scan = new ClickHouseBatchScan(scanJob(table, new ReadOptions(enabled)), readCapture)
      scan.createReaderFactory
      scan.createReaderFactory // e.g. AQE stage reuse: still at most one event per scan
      assert(readCapture.events.map(_.event) === Seq("read"))

      val writeCapture = UserAnalyticsFactory.createCapture()
      val write =
        new ClickHouseBatchWrite(writeJob(table, new WriteOptions(enabled)), isOverwrite = false, writeCapture)
      write.createBatchWriterFactory(onePartition)
      assert(writeCapture.events.map(_.event) === Seq("write"))

      val readEvent = readCapture.events.head
      val writeEvent = writeCapture.events.head
      Seq(readEvent, writeEvent).foreach { event =>
        assert(event.sparkVersion === SPARK_VERSION)
        assert(event.appNameHash === Some(expectedSha256Hex16(spark.sparkContext.appName)))
        assert(event.tableId === Some(expectedTableId(uuid)))
        assert(event.engine === engine)
        assert(event.deployment === (if (isCloud) "cloud" else "self_managed"))

        // this suite runs against the fat runtime jar: the version must survive its composite
        // `<spark>_<scala>_<connector>` manifest, so no `_` may leak through
        val params = event.toParams.toMap
        assert(params("version") !== "unknown")
        assert(!params("version").contains("_"))
      }
      assert(readEvent.format === spark.conf.get("spark.clickhouse.read.format"))
      assert(writeEvent.format === spark.conf.get("spark.clickhouse.write.format"))
    }
  }

  test("spark.clickhouse.sendAnonymousUsageStats=false suppresses reporting") {
    withSimpleTable("usage_stats_db", "usage_stats_opt_out", writeData = false) { (db, tbl) =>
      val table = loadClickHouseTable(db, tbl)
      val disabled = options(SEND_ANONYMOUS_USAGE_STATS.key -> "false")

      val readCapture = UserAnalyticsFactory.createCapture()
      new ClickHouseBatchScan(scanJob(table, new ReadOptions(disabled)), readCapture).createReaderFactory

      val writeCapture = UserAnalyticsFactory.createCapture()
      new ClickHouseBatchWrite(writeJob(table, new WriteOptions(disabled)), isOverwrite = false, writeCapture)
        .createBatchWriterFactory(onePartition)

      assert(readCapture.events.isEmpty)
      assert(writeCapture.events.isEmpty)
    }
  }

  test("the reported runtime comes from the stack of the thread the hook runs on") {
    withSimpleTable("usage_stats_db", "usage_stats_runtime", writeData = false) { (db, tbl) =>
      val table = loadClickHouseTable(db, tbl)
      val enabled = options(SEND_ANONYMOUS_USAGE_STATS.key -> "true")

      val capture = UserAnalyticsFactory.createCapture()
      val scan = new ClickHouseBatchScan(scanJob(table, new ReadOptions(enabled)), capture)
      new DataprocCaller().reportRead(scan)

      assert(capture.events.head.toParams.toMap.get("runtime") === Some("dataproc"))
    }
  }
}

/** Stands in for a managed platform: its name puts a `dataproc` frame on the reporting thread. */
private class DataprocCaller {
  // calls the hook directly; a closure hosted here would plant the frame on the wrong stack
  def reportRead(scan: ClickHouseBatchScan): PartitionReaderFactory = scan.createReaderFactory
}
