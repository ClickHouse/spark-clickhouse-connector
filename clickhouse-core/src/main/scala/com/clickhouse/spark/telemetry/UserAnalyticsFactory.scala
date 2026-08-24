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

/** Creates the [[UserAnalytics]] sinks the calling side passes into the read/write paths. */
private[spark] object UserAnalyticsFactory {

  /** The sink the connector ships with: Scarf. */
  def create(): UserAnalytics = createScarf()

  def createScarf(): UserAnalytics = new ScarfUserAnalytics()

  /** In-memory recorder for suites asserting what the production hooks report. */
  def createCapture(): CaptureUserAnalytics = new CaptureUserAnalytics()
}
