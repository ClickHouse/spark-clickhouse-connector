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

  test("buildUrl carries anonymous version params only") {
    val url = ScarfTelemetry.buildUrl(ScarfTelemetry.EVENT_READ, "3.5.4", "https://example.com/spark-connector")
    assert(url.startsWith("https://example.com/spark-connector?"))
    val params = url.split('?')(1).split('&').map(_.split("=", 2)).map(kv => kv(0) -> kv(1)).toMap
    assert(params("event") === "read")
    assert(params("spark_version") === "3.5.4")
    assert(params("scala_version") === scala.util.Properties.versionNumberString)
    assert(params.keySet === Set("event", "version", "spark_version", "scala_version", "java_version", "os", "arch"))
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
      ScarfTelemetry.reportJobRun(ScarfTelemetry.EVENT_READ, "3.5.4", enabledByConf = false, endpoint, noEnv)
      ScarfTelemetry.reportJobRun(
        ScarfTelemetry.EVENT_READ,
        "3.5.4",
        enabledByConf = true,
        endpoint,
        Map("DO_NOT_TRACK" -> "1").get
      )
      // sent; the telemetry thread is FIFO, so had either dropped event been enqueued it would arrive first
      ScarfTelemetry.reportJobRun(ScarfTelemetry.EVENT_WRITE, "4.0.1", enabledByConf = true, endpoint, noEnv)

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
        ScarfTelemetry.reportJobRun(ScarfTelemetry.EVENT_READ, "3.5.4", enabledByConf = true, endpoint, noEnv)
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
