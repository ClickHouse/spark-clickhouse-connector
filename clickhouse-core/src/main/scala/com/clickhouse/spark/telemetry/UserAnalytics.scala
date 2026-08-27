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

/**
 * Sink for the connector's anonymous usage analytics. Implementations must be fire-and-forget:
 * reporting never throws and never blocks the caller. Instances are created via
 * [[UserAnalyticsFactory]] and passed into the read/write paths that report on them.
 */
private[spark] trait UserAnalytics {

  /** `event` and `enabledByConf` are by-name so implementations only run them behind their opt-out gates. */
  def reportJobRun(event: => UserAnalyticsEvent, enabledByConf: => Boolean): Unit
}
