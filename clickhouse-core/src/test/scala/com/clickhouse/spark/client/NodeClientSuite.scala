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

package com.clickhouse.spark.client

import com.clickhouse.client.ClickHouseProtocol.HTTP
import com.clickhouse.spark.spec.NodeSpec
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, Logger}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.collection.mutable.ArrayBuffer

/**
 * Unit tests for [[NodeClient]] that do not require a running ClickHouse server. Building a
 * `NodeClient` does not open a connection, so these run offline.
 *
 * These cover the failsafe in `NodeClient.clientOptions` (issue #557). Connector settings such as
 * `ssl` are captured as first-class `NodeSpec` fields and stripped upstream, so they never reach the
 * client. As a safety net, `NodeClient` forwards only options the clickhouse-java v2 client
 * recognizes (an allow-list derived from `ClientConfigProperties`) and drops anything else with a
 * WARN, rather than leaking it and letting the client log "Unknown and unmapped config properties".
 * The filtering happens while `Client.build()` parses the config map, so no server is needed.
 */
class NodeClientSuite extends AnyFunSuite {

  private val ConnectorLogger = "com.clickhouse.spark.client.NodeClient"
  private val ClientConfigLogger = "com.clickhouse.client.api.ClientConfigProperties"

  /** Collects log4j events so we can inspect what was logged while building the client. */
  private class CapturingAppender extends AppenderSkeleton {
    val events: ArrayBuffer[LoggingEvent] = ArrayBuffer.empty[LoggingEvent]
    override def append(event: LoggingEvent): Unit = events += event
    override def close(): Unit = ()
    override def requiresLayout(): Boolean = false
  }

  private def captureWarnings(loggerNames: String*)(build: => Unit): Seq[String] = {
    val loggers = loggerNames.map(Logger.getLogger)
    val appender = new CapturingAppender
    val previousLevels = loggers.map(l => l -> l.getLevel)
    loggers.foreach { l => l.setLevel(Level.WARN); l.addAppender(appender) }
    try build
    finally previousLevels.foreach { case (l, level) => l.removeAppender(appender); l.setLevel(level) }
    appender.events
      .filter(_.getLevel == Level.WARN)
      .map(_.getRenderedMessage)
      .toSeq
  }

  /** Builds a NodeClient with the given options and returns the WARN messages it produced. */
  private def warningsForOptions(options: (String, String)*): Seq[String] = {
    val optionMap = new util.HashMap[String, String]()
    options.foreach { case (k, v) => optionMap.put(k, v) }
    val nodeSpec = NodeSpec(
      _host = "localhost",
      _http_port = Some(8443),
      protocol = HTTP,
      options = optionMap
    )
    captureWarnings(ConnectorLogger, ClientConfigLogger) {
      val client = NodeClient(nodeSpec)
      client.close()
    }
  }

  test("unrecognized client options are dropped with a warning") {
    // Failsafe must warn on dropped keys, not swallow typos silently.
    val unknownKey = "definitely_not_a_ch_client_option"
    val warnings = warningsForOptions(unknownKey -> "x")

    assert(
      warnings.exists(_.contains(unknownKey)),
      s"expected a warning about the unrecognized option `$unknownKey`, got: ${warnings.mkString("; ")}"
    )
  }

  test("recognized client options are forwarded without a warning") {
    // `compress` is a genuine clickhouse-java client option; it must pass through untouched.
    val warnings = warningsForOptions("compress" -> "true")

    assert(
      warnings.isEmpty,
      s"a recognized client option should not be warned about, got: ${warnings.mkString("; ")}"
    )
  }
}
