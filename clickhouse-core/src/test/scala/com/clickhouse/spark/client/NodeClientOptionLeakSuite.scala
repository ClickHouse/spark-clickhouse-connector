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
 * Reproduces https://github.com/ClickHouse/spark-clickhouse-connector/issues/557
 *
 * The connector consumes `option.ssl` itself (to choose the http/https URL scheme) but also forwards
 * the whole options map into the clickhouse-java v2 client via `Client.Builder.setOptions`. The v2
 * client validates option keys against `ClientConfigProperties` and logs a WARN for anything it does
 * not recognise. `ssl` is a connector-only option, so it ends up in the "Unknown and unmapped config
 * properties" bucket and gets logged on every client build.
 *
 * Building a `NodeClient` does not open a connection (the warning is emitted while `Client.build()`
 * parses the config map), so this reproduces without a running ClickHouse server.
 */
class NodeClientOptionLeakSuite extends AnyFunSuite {

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

  test("connector-only `ssl` option must not leak into the clickhouse-java client") {
    val options = new util.HashMap[String, String]()
    options.put("ssl", "true")

    val nodeSpec = NodeSpec(
      _host = "localhost",
      _http_port = Some(8443),
      protocol = HTTP,
      options = options
    )

    val warnings = captureClientConfigWarnings {
      val client = NodeClient(nodeSpec)
      client.close()
    }

    val leaked = warnings.filter(msg =>
      msg.contains("Unknown and unmapped config properties") && msg.contains("ssl")
    )

    assert(
      leaked.isEmpty,
      s"clickhouse-java client warned about connector-only options being forwarded: ${leaked.mkString("; ")}"
    )
  }
}
