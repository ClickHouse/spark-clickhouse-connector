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

// under `com.clickhouse.spark` (unlike the sibling suites) to reach `private[spark]` ScarfTelemetry
package com.clickhouse.spark.single

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn, ScarfTelemetryCapture}
import com.clickhouse.spark.{ScarfTelemetry, ScarfTelemetryEvent}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.SEND_ANONYMOUS_USAGE_STATS
import org.apache.spark.sql.clickhouse.single.SparkClickHouseSingleTest
import org.scalatest.tags.Cloud

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

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

  /** Routes the production call sites' telemetry to `capture`, bypassing the test JVM's `SCARF_NO_ANALYTICS`. */
  private def withCapturedTelemetry(capture: ScarfTelemetryCapture)(f: => Unit): Unit = {
    val (endpointBefore, envBefore) = (ScarfTelemetry.defaultEndpoint, ScarfTelemetry.defaultEnv)
    ScarfTelemetry.defaultEndpoint = capture.endpoint
    ScarfTelemetry.defaultEnv = _ => None
    try f
    finally {
      // reverse order of the setup: re-arm the env opt-out before the endpoint goes back to production
      ScarfTelemetry.defaultEnv = envBefore
      ScarfTelemetry.defaultEndpoint = endpointBefore
    }
  }

  /** The telemetry thread is FIFO: once the `done` event arrives, everything real jobs enqueued has arrived too. */
  private def reportDone(): Unit =
    ScarfTelemetry.reportJobRun(
      ScarfTelemetryEvent(
        event = "done",
        sparkVersion = SPARK_VERSION,
        appNameHash = None,
        tableId = None,
        deployment = "self_managed",
        format = "json",
        engine = "MergeTree",
        convertLocal = false
      ),
      enabledByConf = true
    )

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

  test("a real write and a real read job each send one event carrying the job's metadata") {
    ScarfTelemetryCapture.withCapture { capture =>
      withCapturedTelemetry(capture) {
        withSimpleTable("scarf_tel_db", "scarf_tel_tbl", writeData = true) { (db, tbl) => // a real write
          spark.sql(s"SELECT * FROM `$db`.`$tbl`").collect() // a real read
          reportDone()

          val events = capture.awaitEvents(3)
          assert(events.map(_.params("event")) === Seq("write", "read", "done"))

          val (uuid, engine) = tableUuidAndEngine(db, tbl)
          val writeParams = events(0).params
          val readParams = events(1).params
          Seq(writeParams, readParams).foreach { params =>
            assert(params("spark_version") === SPARK_VERSION)
            assert(params("app_name_hash") === expectedSha256Hex16(spark.sparkContext.appName))
            assert(params("table_id") === expectedTableId(uuid))
            assert(params("engine") === engine)
            assert(params("deployment") === (if (isCloud) "cloud" else "self_managed"))
            // this suite runs against the fat runtime jar: versions must survive its composite
            // `<spark>_<scala>_<connector>` manifest, so no `_` may leak through
            assert(params("version") !== "unknown")
            assert(!params("version").contains("_"))
            assert(!params("client_version").contains("_"))
            assert(params("client_version").matches("""\d+\.\d+.*"""))
          }
          assert(readParams("format") === spark.conf.get("spark.clickhouse.read.format"))
          assert(writeParams("format") === spark.conf.get("spark.clickhouse.write.format"))
          assert(readParams("convert_local") === spark.conf.get("spark.clickhouse.read.distributed.convertLocal"))
          assert(writeParams("convert_local") === spark.conf.get("spark.clickhouse.write.distributed.convertLocal"))
        }
      }
    }
  }

  test("spark.clickhouse.sendAnonymousUsageStats=false suppresses events from real jobs") {
    ScarfTelemetryCapture.withCapture { capture =>
      withCapturedTelemetry(capture) {
        spark.conf.set(SEND_ANONYMOUS_USAGE_STATS.key, "false")
        try
          withSimpleTable("scarf_tel_db", "scarf_tel_opt_out", writeData = true) { (db, tbl) =>
            spark.sql(s"SELECT * FROM `$db`.`$tbl`").collect()
          }
        finally spark.conf.unset(SEND_ANONYMOUS_USAGE_STATS.key)
        reportDone()

        val events = capture.awaitEvents(1)
        assert(events.map(_.params("event")) === Seq("done"))
      }
    }
  }
}
