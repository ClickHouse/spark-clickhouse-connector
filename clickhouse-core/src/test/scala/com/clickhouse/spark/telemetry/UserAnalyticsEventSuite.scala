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

import com.clickhouse.spark.spec.NodeSpec
import org.scalatest.funsuite.AnyFunSuite

class UserAnalyticsEventSuite extends AnyFunSuite {

  test("sha256Hex16 - one-way hash, first 16 hex chars") {
    // sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
    assert(UserAnalyticsEvent.sha256Hex16("abc") === "ba7816bf8f01cfea")
    assert(UserAnalyticsEvent.sha256Hex16("nightly_sales_load").length === 16)
  }

  test("tableId - truncated raw UUID, zero UUID omitted") {
    assert(UserAnalyticsEvent.tableId("12345678-90ab-cdef-1234-567890abcdef") === Some("1234567890abcdef"))
    assert(UserAnalyticsEvent.tableId("00000000-0000-0000-0000-000000000000") === None)
    assert(UserAnalyticsEvent.tableId("") === None)
    assert(UserAnalyticsEvent.tableId(null) === None)
  }

  test("parseConnectorVersion - composite runtime-jar version yields the connector part") {
    assert(UserAnalyticsEvent.parseConnectorVersion("3.5_2.13_0.10.0") === "0.10.0")
    assert(UserAnalyticsEvent.parseConnectorVersion("4.0_2.13_0.10.1-SNAPSHOT") === "0.10.1-SNAPSHOT")
  }

  test("parseConnectorVersion - plain core-jar version is kept as is") {
    assert(UserAnalyticsEvent.parseConnectorVersion("0.10.1") === "0.10.1")
    assert(UserAnalyticsEvent.parseConnectorVersion("0.10.1-SNAPSHOT") === "0.10.1-SNAPSHOT")
  }

  test("clientVersion is the client-v2 runtime version") {
    assert(UserAnalyticsEvent.clientVersion.matches("""\d+\.\d+\.\d+.*"""))
  }

  test("serverVersion - looked up per server, blank versions ignored") {
    // a host of its own: the registry is JVM-wide
    val node = NodeSpec("server-version-suite", Some(8123))
    UserAnalyticsEvent.recordServerVersion(node, " ")
    assert(UserAnalyticsEvent.serverVersion(node) === None)
    UserAnalyticsEvent.recordServerVersion(node, "25.3.14.14")
    assert(UserAnalyticsEvent.serverVersion(node) === Some("25.3.14.14"))
    assert(UserAnalyticsEvent.serverVersion(NodeSpec("server-version-suite", Some(8124))) === None)
    assert(UserAnalyticsEvent.serverVersion(NodeSpec("server-version-suite")) === None) // no port: must not throw
  }
}
