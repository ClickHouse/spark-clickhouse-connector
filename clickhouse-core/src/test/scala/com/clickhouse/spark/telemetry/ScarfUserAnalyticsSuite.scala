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

package com.clickhouse.spark.telemetry

import com.clickhouse.spark.base.ScarfUserAnalyticsCapture
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ScarfUserAnalyticsSuite extends AnyFunSuite {

  private val noEnv: String => Option[String] = _ => None

  private def sampleEvent(
    event: String = UserAnalyticsEvent.EVENT_READ,
    sparkVersion: String = "3.5.4"
  ): UserAnalyticsEvent =
    UserAnalyticsEvent(
      event = event,
      sparkVersion = sparkVersion,
      appNameHash = Some(UserAnalyticsEvent.sha256Hex16("nightly_sales_load")),
      tableId = UserAnalyticsEvent.tableId("12345678-90ab-cdef-1234-567890abcdef"),
      format = "json",
      engine = "MergeTree"
    )

  test("disabledByEnv - not disabled by default") {
    assert(!ScarfUserAnalytics.disabledByEnv(noEnv))
  }

  test("disabledByEnv - SCARF_NO_ANALYTICS") {
    assert(ScarfUserAnalytics.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "true").get))
    assert(ScarfUserAnalytics.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "TRUE").get))
    assert(ScarfUserAnalytics.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "1").get))
    assert(!ScarfUserAnalytics.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "false").get))
    assert(!ScarfUserAnalytics.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "0").get))
  }

  test("disabledByEnv - DO_NOT_TRACK") {
    assert(ScarfUserAnalytics.disabledByEnv(Map("DO_NOT_TRACK" -> "true").get))
    assert(ScarfUserAnalytics.disabledByEnv(Map("DO_NOT_TRACK" -> "1").get))
    assert(!ScarfUserAnalytics.disabledByEnv(Map("DO_NOT_TRACK" -> "false").get))
  }

  test("buildUrl carries the expected params and omits absent ones") {
    val url = ScarfUserAnalytics.buildUrl(sampleEvent(), "https://example.com/spark-connector")
    assert(url.startsWith("https://example.com/spark-connector?"))
    val params = url.split('?')(1).split('&').map(_.split("=", 2)).map(kv => kv(0) -> kv(1)).toMap
    assert(params("event") === "read")
    assert(params("spark_version") === "3.5.4")
    assert(params("format") === "json")
    assert(params("engine") === "MergeTree")
    assert(params("test") === "true") // the suite itself runs under a test framework
    assert(params("app_name_hash") === UserAnalyticsEvent.sha256Hex16("nightly_sales_load"))
    assert(params("table_id") === "1234567890abcdef")
    // `runtime` is present only when a managed platform is detected
    assert(
      (params.keySet - "runtime") === Set(
        "event",
        "version",
        "spark_version",
        "os",
        "format",
        "engine",
        "app_name_hash",
        "table_id",
        "test"
      )
    )

    val minimal = sampleEvent().copy(appNameHash = None, tableId = None)
    val minimalParams = ScarfUserAnalytics.buildUrl(minimal, "https://example.com/x")
      .split('?')(1).split('&').map(_.split("=", 2)(0)).toSet
    assert(!minimalParams.contains("app_name_hash"))
    assert(!minimalParams.contains("table_id"))
  }

  test("reportJobRun sends a GET only when enabled by conf and env") {
    ScarfUserAnalyticsCapture.withCapture { capture =>
      // dropped: disabled by conf, then disabled by env
      new ScarfUserAnalytics(capture.endpoint, noEnv).reportJobRun(sampleEvent(), enabledByConf = false)
      new ScarfUserAnalytics(capture.endpoint, Map("DO_NOT_TRACK" -> "1").get)
        .reportJobRun(sampleEvent(), enabledByConf = true)
      // sent; all instances share one FIFO delivery thread, so had either dropped
      // event been enqueued it would arrive first
      new ScarfUserAnalytics(capture.endpoint, noEnv)
        .reportJobRun(sampleEvent(event = UserAnalyticsEvent.EVENT_WRITE, sparkVersion = "4.0.1"), enabledByConf = true)

      val events = capture.awaitEvents(1)
      assert(events.size === 1)
      assert(events.head.params("event") === "write")
      assert(events.head.params("spark_version") === "4.0.1")
      assert(events.head.userAgent.startsWith("spark-clickhouse-connector/"))
    }
  }

  test("reportJobRun returns immediately even when the endpoint hangs for 30 seconds") {
    val requestReceived = new CountDownLatch(1)
    val releaseResponse = new CountDownLatch(1)

    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/spark-connector",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          requestReceived.countDown()
          releaseResponse.await(30, TimeUnit.SECONDS) // hang the response for 30 seconds
          exchange.sendResponseHeaders(200, -1)
          exchange.close()
        }
      }
    )
    server.start()
    try {
      val scarf =
        new ScarfUserAnalytics(s"http://127.0.0.1:${server.getAddress.getPort}/spark-connector", noEnv)

      // 10 events against the hanging endpoint also fill the send queue and hit the discard
      // path; a blocking implementation would need the 3s read timeout per event (30s total)
      val startNanos = System.nanoTime()
      (1 to 10).foreach { _ =>
        scarf.reportJobRun(sampleEvent(), enabledByConf = true)
      }
      val elapsedMs = (System.nanoTime() - startNanos) / 1000000L

      assert(elapsedMs < 2000, s"reportJobRun blocked the caller for ${elapsedMs}ms")
      // the events really were sent to the hanging endpoint, not skipped
      assert(requestReceived.await(10, TimeUnit.SECONDS))
    } finally {
      releaseResponse.countDown() // unblock the handler so shutdown does not wait 30 seconds
      server.stop(0)
    }
  }

  test("reportJobRun swallows exceptions thrown while resolving the conf") {
    var evaluated = false
    new ScarfUserAnalytics("http://127.0.0.1:1/spark-connector", noEnv).reportJobRun(
      sampleEvent(),
      enabledByConf = { evaluated = true; throw new IllegalArgumentException("not a boolean") }
    )
    assert(evaluated)
  }

  test("reportJobRun swallows LinkageErrors raised while building the event") {
    var evaluated = false
    new ScarfUserAnalytics("http://127.0.0.1:1/spark-connector", noEnv).reportJobRun(
      { evaluated = true; throw new NoClassDefFoundError("com/example/PlatformProbeParent") },
      enabledByConf = true
    )
    assert(evaluated)
  }
}
