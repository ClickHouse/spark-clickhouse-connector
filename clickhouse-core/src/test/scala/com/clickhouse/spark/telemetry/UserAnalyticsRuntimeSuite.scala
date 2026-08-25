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

import com.clickhouse.spark.Utils
import org.scalatest.funsuite.AnyFunSuite

class UserAnalyticsRuntimeSuite extends AnyFunSuite {
  import UserAnalyticsRuntimeSuite._

  test("stack trace detection only sees the platform on the thread that reports the event") {
    assert(new DataprocProbe().detectHere() === Some("Dataproc"))
    assert(detectOnFreshThread() === None)
  }

  test("the runtime reported by the caller's stack survives building the params off that thread") {
    val event = new DataprocProbe().buildEvent()
    assert(paramsOnFreshThread(event).get("runtime") === Some("dataproc"))
  }
}

private object UserAnalyticsRuntimeSuite {

  /**
   * Runs `body` on a fresh thread. Closures are hosted here rather than in [[DataprocProbe]] so
   * the new thread's stack cannot inherit a platform-named lambda frame from it.
   */
  private def onFreshThread[A](body: => A): A = {
    var result: Option[A] = None
    val thread = new Thread(() => result = Some(body), "not-the-reporting-thread")
    thread.start()
    thread.join()
    result.get
  }

  def detectOnFreshThread(): Option[String] = onFreshThread(Utils.RuntimeDetector.detectViaStackTrace())

  def paramsOnFreshThread(event: UserAnalyticsEvent): Map[String, String] = onFreshThread(event.toParams.toMap)

  /** Puts a `dataproc` frame on the stack of whatever it calls, standing in for a managed platform. */
  class DataprocProbe {

    def detectHere(): Option[String] = Utils.RuntimeDetector.detectViaStackTrace()

    def buildEvent(): UserAnalyticsEvent =
      UserAnalyticsEvent(
        event = UserAnalyticsEvent.EVENT_READ,
        sparkVersion = "4.0.1",
        appNameHash = None,
        tableId = None,
        deployment = "self_managed",
        format = "json",
        engine = "MergeTree",
        convertLocal = false
      )
  }
}
