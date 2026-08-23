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

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ScarfTelemetrySuite extends AnyFunSuite {

  private val noEnv: String => Option[String] = _ => None

  private def sampleEvent(
    event: String = ScarfTelemetryEvent.EVENT_READ,
    sparkVersion: String = "3.5.4"
  ): ScarfTelemetryEvent =
    ScarfTelemetryEvent(
      event = event,
      sparkVersion = sparkVersion,
      appNameHash = Some(ScarfTelemetryEvent.sha256Hex16("nightly_sales_load")),
      tableId = ScarfTelemetryEvent.tableId("12345678-90ab-cdef-1234-567890abcdef"),
      deployment = ScarfTelemetryEvent.deployment("ch.internal.corp"),
      format = "json",
      engine = "MergeTree",
      convertLocal = false
    )

  test("disabledByEnv - not disabled by default") {
    assert(!ScarfTelemetry.disabledByEnv(noEnv))
  }

  test("disabledByEnv - SCARF_NO_ANALYTICS") {
    assert(ScarfTelemetry.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "true").get))
    assert(ScarfTelemetry.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "TRUE").get))
    assert(ScarfTelemetry.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "1").get))
    assert(!ScarfTelemetry.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "false").get))
    assert(!ScarfTelemetry.disabledByEnv(Map("SCARF_NO_ANALYTICS" -> "0").get))
  }

  test("disabledByEnv - DO_NOT_TRACK") {
    assert(ScarfTelemetry.disabledByEnv(Map("DO_NOT_TRACK" -> "true").get))
    assert(ScarfTelemetry.disabledByEnv(Map("DO_NOT_TRACK" -> "1").get))
    assert(!ScarfTelemetry.disabledByEnv(Map("DO_NOT_TRACK" -> "false").get))
  }

  test("sha256Hex16 - one-way hash, first 16 hex chars") {
    // sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
    assert(ScarfTelemetryEvent.sha256Hex16("abc") === "ba7816bf8f01cfea")
    assert(ScarfTelemetryEvent.sha256Hex16("nightly_sales_load").length === 16)
  }

  test("tableId - truncated raw UUID, zero UUID omitted") {
    assert(ScarfTelemetryEvent.tableId("12345678-90ab-cdef-1234-567890abcdef") === Some("1234567890abcdef"))
    assert(ScarfTelemetryEvent.tableId("00000000-0000-0000-0000-000000000000") === None)
    assert(ScarfTelemetryEvent.tableId("") === None)
    assert(ScarfTelemetryEvent.tableId(null) === None)
  }

  test("deployment - derived from hostname suffix only") {
    assert(ScarfTelemetryEvent.deployment("abc123.us-east-1.aws.clickhouse.cloud") === "cloud")
    assert(ScarfTelemetryEvent.deployment("ABC.CLICKHOUSE.CLOUD") === "cloud")
    assert(ScarfTelemetryEvent.deployment("ch.prod.internal.corp") === "self_managed")
    assert(ScarfTelemetryEvent.deployment("10.1.2.3") === "self_managed")
    assert(ScarfTelemetryEvent.deployment(null) === "self_managed")
  }

  test("buildUrl carries the expected params and omits absent ones") {
    val url = ScarfTelemetry.buildUrl(sampleEvent(), "https://example.com/spark-connector")
    assert(url.startsWith("https://example.com/spark-connector?"))
    val params = url.split('?')(1).split('&').map(_.split("=", 2)).map(kv => kv(0) -> kv(1)).toMap
    assert(params("event") === "read")
    assert(params("spark_version") === "3.5.4")
    assert(params("scala_version") === scala.util.Properties.versionNumberString)
    assert(params("run_id").length === 36)
    assert(params("deployment") === "self_managed")
    assert(params("format") === "json")
    assert(params("engine") === "MergeTree")
    assert(params("convert_local") === "false")
    assert(params("app_name_hash") === ScarfTelemetryEvent.sha256Hex16("nightly_sales_load"))
    assert(params("table_id") === "1234567890abcdef")
    // `runtime` is present only when a managed platform is detected
    assert(
      (params.keySet - "runtime") === Set(
        "event",
        "version",
        "spark_version",
        "scala_version",
        "java_version",
        "os",
        "arch",
        "run_id",
        "deployment",
        "format",
        "engine",
        "convert_local",
        "client_version",
        "app_name_hash",
        "table_id"
      )
    )

    val minimal = sampleEvent().copy(appNameHash = None, tableId = None)
    val minimalParams =
      ScarfTelemetry.buildUrl(minimal, "https://example.com/x").split('?')(1).split('&').map(_.split("=", 2)(0)).toSet
    assert(!minimalParams.contains("app_name_hash"))
    assert(!minimalParams.contains("table_id"))
  }

  test("run_id is stable within the JVM") {
    val params1 = sampleEvent().toQueryParams.toMap
    val params2 = sampleEvent(event = ScarfTelemetryEvent.EVENT_WRITE).toQueryParams.toMap
    assert(params1("run_id") === params2("run_id"))
  }

  test("reportJobRun sends a GET only when enabled by conf and env") {
    val requestCount = new AtomicInteger(0)
    val latch = new CountDownLatch(1)
    @volatile var query: String = null
    @volatile var userAgent: String = null

    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/spark-connector",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          requestCount.incrementAndGet()
          query = exchange.getRequestURI.getQuery
          userAgent = exchange.getRequestHeaders.getFirst("User-Agent")
          exchange.sendResponseHeaders(200, -1)
          exchange.close()
          latch.countDown()
        }
      }
    )
    server.start()
    try {
      val endpoint = s"http://127.0.0.1:${server.getAddress.getPort}/spark-connector"
      // dropped: disabled by conf, then disabled by env
      ScarfTelemetry.reportJobRun(sampleEvent(), enabledByConf = false, endpoint, noEnv)
      ScarfTelemetry.reportJobRun(sampleEvent(), enabledByConf = true, endpoint, Map("DO_NOT_TRACK" -> "1").get)
      // sent; the telemetry thread is FIFO, so had either dropped event been enqueued it would arrive first
      ScarfTelemetry.reportJobRun(
        sampleEvent(event = ScarfTelemetryEvent.EVENT_WRITE, sparkVersion = "4.0.1"),
        enabledByConf = true,
        endpoint,
        noEnv
      )

      assert(latch.await(30, TimeUnit.SECONDS))
      assert(requestCount.get === 1)
      assert(query.contains("event=write"))
      assert(query.contains("spark_version=4.0.1"))
      assert(userAgent.startsWith("spark-clickhouse-connector/"))
    } finally
      server.stop(0)
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
      val endpoint = s"http://127.0.0.1:${server.getAddress.getPort}/spark-connector"

      // 10 events against the hanging endpoint also fill the send queue and hit the discard
      // path; a blocking implementation would need the 3s read timeout per event (30s total)
      val startNanos = System.nanoTime()
      (1 to 10).foreach { _ =>
        ScarfTelemetry.reportJobRun(sampleEvent(), enabledByConf = true, endpoint, noEnv)
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
}
