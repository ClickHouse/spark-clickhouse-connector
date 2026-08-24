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

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

/** [[UserAnalytics]] recording conf-enabled events in memory; created via [[UserAnalyticsFactory.createCapture]]. */
private[spark] class CaptureUserAnalytics extends UserAnalytics {

  private val captured = new ConcurrentLinkedQueue[UserAnalyticsEvent]

  override def reportJobRun(event: => UserAnalyticsEvent, enabledByConf: => Boolean): Unit =
    if (enabledByConf) captured.add(event)

  def events: Seq[UserAnalyticsEvent] = captured.asScala.toList
}
