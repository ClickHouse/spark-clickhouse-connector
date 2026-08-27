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

package com.clickhouse.spark.base

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{InetSocketAddress, URLDecoder}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

/**
 * Local HTTP server capturing the Scarf usage-analytics GETs sent to [[endpoint]]. The delivery
 * thread is FIFO, so suites can prove nothing else was enqueued with a trailing marker event.
 */
class ScarfUserAnalyticsCapture private (server: HttpServer) {
  import ScarfUserAnalyticsCapture._

  private val captured = new ConcurrentLinkedQueue[CapturedEvent]

  def endpoint: String = s"http://127.0.0.1:${server.getAddress.getPort}/spark-connector"

  def events: Seq[CapturedEvent] = captured.asScala.toList

  /** Polls until `count` events arrived or 30 seconds elapsed, then returns everything captured. */
  def awaitEvents(count: Int): Seq[CapturedEvent] = {
    val deadline = System.nanoTime() + 30L * 1000 * 1000 * 1000
    while (captured.size() < count && System.nanoTime() < deadline) Thread.sleep(50)
    events
  }
}

object ScarfUserAnalyticsCapture {

  case class CapturedEvent(params: Map[String, String], userAgent: String)

  def withCapture[T](f: ScarfUserAnalyticsCapture => T): T = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val capture = new ScarfUserAnalyticsCapture(server)
    server.createContext(
      "/spark-connector",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          capture.captured.add(CapturedEvent(
            parseQuery(exchange.getRequestURI.getRawQuery),
            exchange.getRequestHeaders.getFirst("User-Agent")
          ))
          exchange.sendResponseHeaders(200, -1)
          exchange.close()
        }
      }
    )
    server.start()
    try f(capture)
    finally server.stop(0)
  }

  private def parseQuery(rawQuery: String): Map[String, String] =
    Option(rawQuery).map(_.split('&').toSeq).getOrElse(Seq.empty)
      .map(_.split("=", 2))
      .collect { case Array(key, value) => URLDecoder.decode(key, "UTF-8") -> URLDecoder.decode(value, "UTF-8") }
      .toMap
}
