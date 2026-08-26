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

import org.scalatest.funsuite.AnyFunSuite

class UserAnalyticsFactorySuite extends AnyFunSuite {

  private def sampleEvent(event: String): UserAnalyticsEvent =
    UserAnalyticsEvent(
      event = event,
      sparkVersion = "3.5.4",
      appNameHash = None,
      tableId = None,
      deployment = "self_managed",
      format = "json",
      engine = "MergeTree"
    )

  test("create returns the shipped default: Scarf") {
    assert(UserAnalyticsFactory.create().isInstanceOf[ScarfUserAnalytics])
  }

  test("createCapture records only conf-enabled events, in order") {
    val capture = UserAnalyticsFactory.createCapture()
    capture.reportJobRun(sampleEvent("read"), enabledByConf = true)
    capture.reportJobRun(sampleEvent("write"), enabledByConf = false)
    capture.reportJobRun(sampleEvent("write"), enabledByConf = true)
    assert(capture.events.map(_.event) === Seq("read", "write"))
  }

  test("createCapture swallows conf and event failures instead of throwing") {
    val capture = UserAnalyticsFactory.createCapture()
    capture.reportJobRun(sampleEvent("read"), enabledByConf = throw new IllegalArgumentException("not a boolean"))
    capture.reportJobRun(throw new NoClassDefFoundError("com/example/PlatformProbeParent"), enabledByConf = true)
    assert(capture.events.isEmpty)
  }
}
