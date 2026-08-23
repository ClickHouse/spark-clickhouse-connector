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

package com.clickhouse.spark.single

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn}
import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.read.ScanJobDescription
import com.clickhouse.spark.write.WriteJobDescription
import com.clickhouse.spark.{ClickHouseCatalog, ClickHouseTable, ScarfTelemetry, ScarfTelemetryEvents, Utils}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.clickhouse.single.SparkClickHouseSingleTest
import org.apache.spark.sql.clickhouse.{ReadOptions, WriteOptions}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.tags.Cloud

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

@Cloud
class ClickHouseCloudScarfTelemetryEventsSuite extends ScarfTelemetryEventsSuite with ClickHouseCloudMixIn

class ClickHouseSingleScarfTelemetryEventsSuite extends ScarfTelemetryEventsSuite with ClickHouseSingleMixIn

abstract class ScarfTelemetryEventsSuite extends SparkClickHouseSingleTest {

  // independent re-implementations, so the assertions do not reuse the production derivations
  private def expectedSha256Hex16(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(b => f"$b%02x").mkString.take(16)

  private def expectedTableId(uuid: String): String = uuid.toLowerCase.replace("-", "").take(16)

  test("read and write events carry the actual job metadata") {
    withSimpleTable("scarf_tel_db", "scarf_tel_tbl", writeData = true) { (db, tbl) =>
      spark.sql(s"SELECT * FROM `$db`.`$tbl`").collect() // a real read through the connector

      val table = loadClickHouseTable(db, tbl)

      // ground truth fetched from the live server, independently of the catalog metadata
      val row = Utils.tryWithResource(NodeClient(table.node)) { client =>
        client.syncQueryAndCheckOutputJSONEachRow(
          s"SELECT uuid, engine FROM system.tables WHERE database = '$db' AND name = '$tbl'"
        ).records.head
      }
      val (uuid, engine) = (row.get("uuid").asText, row.get("engine").asText)
      assert(table.spec.uuid === uuid)
      val expectedDeployment = if (isCloud) "cloud" else "self_managed"

      val readEvent = ScarfTelemetryEvents.readEvent(ScanJobDescription(
        node = table.node,
        tz = table.tz,
        tableSpec = table.spec,
        tableEngineSpec = table.engineSpec,
        cluster = table.cluster,
        localTableSpec = table.localTableSpec,
        localTableEngineSpec = table.localTableEngineSpec,
        readOptions = new ReadOptions(java.util.Collections.emptyMap()),
        functionRegistry = table.functionRegistry
      ))
      val writeEvent = ScarfTelemetryEvents.writeEvent(WriteJobDescription(
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
        writeOptions = new WriteOptions(java.util.Collections.emptyMap()),
        writeSettings = Map.empty,
        functionRegistry = table.functionRegistry
      ))

      assert(readEvent.event === "read")
      assert(writeEvent.event === "write")
      Seq(readEvent, writeEvent).foreach { event =>
        assert(event.sparkVersion === SPARK_VERSION)
        assert(event.appNameHash === Some(expectedSha256Hex16(spark.sparkContext.appName)))
        assert(event.tableId === Some(expectedTableId(uuid)))
        assert(event.engine === engine)
        assert(event.deployment === expectedDeployment)
      }
      assert(readEvent.format === spark.conf.get("spark.clickhouse.read.format"))
      assert(writeEvent.format === spark.conf.get("spark.clickhouse.write.format"))
      assert(readEvent.convertLocal.toString === spark.conf.get("spark.clickhouse.read.distributed.convertLocal"))
      assert(writeEvent.convertLocal.toString === spark.conf.get("spark.clickhouse.write.distributed.convertLocal"))

      // and over the wire: both real events land with the real metadata
      val queries = new ConcurrentLinkedQueue[String]()
      val latch = new CountDownLatch(2)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/spark-connector",
        new HttpHandler {
          override def handle(exchange: HttpExchange): Unit = {
            queries.add(exchange.getRequestURI.getQuery)
            exchange.sendResponseHeaders(200, -1)
            exchange.close()
            latch.countDown()
          }
        }
      )
      server.start()
      try {
        val endpoint = s"http://127.0.0.1:${server.getAddress.getPort}/spark-connector"
        ScarfTelemetry.reportJobRun(readEvent, enabledByConf = true, endpoint, _ => None)
        ScarfTelemetry.reportJobRun(writeEvent, enabledByConf = true, endpoint, _ => None)
        assert(latch.await(30, TimeUnit.SECONDS))

        val paramMaps = queries.toArray(Array.empty[String]).toSeq
          .map(_.split('&').map(_.split("=", 2)).map(kv => kv(0) -> kv(1)).toMap)
        assert(paramMaps.map(_("event")).toSet === Set("read", "write"))
        paramMaps.foreach { params =>
          assert(params("table_id") === expectedTableId(uuid))
          assert(params("app_name_hash") === expectedSha256Hex16(spark.sparkContext.appName))
          assert(params("engine") === engine)
          assert(params("deployment") === expectedDeployment)
          assert(params("spark_version") === SPARK_VERSION)
        }
      } finally
        server.stop(0)
    }
  }

  /** Loads the table through the production catalog path: `initialize` + `loadTable`. */
  private def loadClickHouseTable(db: String, tbl: String): ClickHouseTable = {
    val catalogOptions = new java.util.HashMap[String, String]()
    cmdRunnerOptions.foreach { case (key, value) => catalogOptions.put(key, value) }
    val catalog = new ClickHouseCatalog()
    catalog.initialize("clickhouse", new CaseInsensitiveStringMap(catalogOptions))
    catalog.loadTable(Identifier.of(Array(db), tbl)).asInstanceOf[ClickHouseTable]
  }
}
