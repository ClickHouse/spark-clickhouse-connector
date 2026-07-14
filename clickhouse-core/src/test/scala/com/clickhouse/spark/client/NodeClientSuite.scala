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
 * The option-forwarding tests cover issue #557: the connector consumes `option.ssl` itself (to
 * choose the http/https URL scheme) but must not forward it to the clickhouse-java v2 client, which
 * validates option keys against `ClientConfigProperties` and logs a WARN ("Unknown and unmapped
 * config properties") for anything it does not recognise. The warning is emitted while
 * `Client.build()` parses the config map, which is why no server is needed.
 */
class NodeClientSuite extends AnyFunSuite {

  /** Collects log4j events so we can inspect what the clickhouse-java client logged. */
  private class CapturingAppender extends AppenderSkeleton {
    val events: ArrayBuffer[LoggingEvent] = ArrayBuffer.empty[LoggingEvent]
    override def append(event: LoggingEvent): Unit = events += event
    override def close(): Unit = ()
    override def requiresLayout(): Boolean = false
  }

  private def captureClientConfigWarnings(build: => Unit): Seq[String] = {
    val logger = Logger.getLogger("com.clickhouse.client.api.ClientConfigProperties")
    val appender = new CapturingAppender
    val previousLevel = logger.getLevel
    logger.setLevel(Level.WARN)
    logger.addAppender(appender)
    try build
    finally {
      logger.removeAppender(appender)
      logger.setLevel(previousLevel)
    }
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
    captureClientConfigWarnings {
      val client = NodeClient(nodeSpec)
      client.close()
    }
  }

  test("connector-only `ssl` option must not leak into the clickhouse-java client") {
    val leaked = warningsForOptions("ssl" -> "true").filter(msg =>
      msg.contains("Unknown and unmapped config properties") && msg.contains("ssl")
    )

    assert(
      leaked.isEmpty,
      s"clickhouse-java client warned about connector-only options being forwarded: ${leaked.mkString("; ")}"
    )
  }

  test("stripping is limited to connector-consumed keys; unknown client options still surface") {
    // Guard against an over-broad fix: filtering out `ssl` must not suppress the client's own
    // validation of genuinely-unknown option keys.
    val unknownKey = "definitely_not_a_ch_client_option"
    val unmapped = warningsForOptions("ssl" -> "true", unknownKey -> "x")
      .filter(_.contains("Unknown and unmapped config properties"))

    assert(
      unmapped.forall(!_.contains("ssl=")),
      s"connector-only `ssl` still leaked: ${unmapped.mkString("; ")}"
    )
    assert(
      unmapped.exists(_.contains(unknownKey)),
      s"expected the client to still warn about the unknown option `$unknownKey`, got: ${unmapped.mkString("; ")}"
    )
  }
}
