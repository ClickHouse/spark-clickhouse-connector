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
import com.clickhouse.spark.LogCaptureHelper
import com.clickhouse.spark.spec.NodeSpec
import org.scalatest.funsuite.AnyFunSuite

import java.util

/**
 * Unit tests for [[NodeClient]] that do not require a running ClickHouse server. Building a
 * `NodeClient` does not open a connection, so these run offline.
 *
 * Options are handed to clickhouse-java untouched: it knows its own option keys and warns about and
 * drops the rest, which is what this asserts (issue #557). That connector settings never reach the
 * client in the first place is covered by `ClickHouseHelperSuite`. The validation happens while
 * `Client.build()` parses the config map, so no server is needed.
 */
class NodeClientSuite extends AnyFunSuite with LogCaptureHelper {

  private val ConnectorLogger = "com.clickhouse.spark.client.NodeClient"
  private val ClientConfigLogger = "com.clickhouse.client.api.ClientConfigProperties"

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

  private val UnknownKey = "definitely_not_a_ch_client_option"

  test("unrecognized client options are dropped with a warning") {
    // A typo must be reported, not swallowed. Deliberately ungated: from clickhouse-java 0.9.7 on the
    // client throws here instead, and this test must fail until `ignore_unknown_config_key=true` is
    // passed in `NodeClient`.
    val warnings = warningsForOptions(UnknownKey -> "x")

    assert(
      warnings.exists(_.contains(UnknownKey)),
      s"expected a warning about the unrecognized option `$UnknownKey`, got: ${warnings.mkString("; ")}"
    )
  }
}
