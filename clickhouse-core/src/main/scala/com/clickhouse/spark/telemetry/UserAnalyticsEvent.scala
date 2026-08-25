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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/**
 * The metadata carried by one anonymous usage event: what happened (read/write), the versions
 * involved, and coarse, non-identifying runtime facts. Identity-adjacent inputs never leave
 * the driver raw: the Spark application name is one-way hashed and the ClickHouse table UUID
 * (random by construction) is truncated. Delivery is owned by the [[UserAnalytics]] sink.
 */
case class UserAnalyticsEvent(
  event: String,
  sparkVersion: String,
  appNameHash: Option[String],
  tableId: Option[String],
  deployment: String,
  format: String,
  engine: String
) {
  import UserAnalyticsEvent._

  // per event, not cached: only the reporting thread's stack shows the platform driving the job
  private val runtimeFromStack: Option[String] = Utils.RuntimeDetector.detectViaStackTrace()

  def toParams: Seq[(String, String)] =
    Seq(
      "event" -> event,
      "version" -> connectorVersion,
      "spark_version" -> sparkVersion,
      "os" -> sys.props.getOrElse("os.name", "unknown"),
      "deployment" -> deployment,
      "format" -> format,
      "engine" -> engine
    ) ++
      appNameHash.map("app_name_hash" -> _) ++
      tableId.map("table_id" -> _) ++
      (runtimeFromStack orElse runtimeFromEnvironment).map(r => "runtime" -> r.toLowerCase)
}

object UserAnalyticsEvent {

  val EVENT_READ: String = "read"
  val EVENT_WRITE: String = "write"

  // tables in legacy `Ordinary` databases carry the zero UUID, which identifies nothing
  private val ZERO_UUID = "00000000-0000-0000-0000-000000000000"
  private val CLOUD_HOST_SUFFIX = ".clickhouse.cloud"

  // `getPackage` and `Implementation-Version` are absent on class dirs
  private[spark] lazy val connectorVersion: String =
    Option(getClass.getPackage).flatMap(p => Option(p.getImplementationVersion))
      .map(parseConnectorVersion)
      .getOrElse("unknown")

  /** `Implementation-Version` is `<spark>_<scala>_<connector>` on the runtime jar, plain on the core jar. */
  private[spark] def parseConnectorVersion(implementationVersion: String): String =
    implementationVersion.split("_") match {
      case Array(_, _, connector, _*) => connector
      case _ => implementationVersion
    }

  // walks class loaders and every thread name, so it stays off the reporting thread; once per JVM
  private lazy val runtimeFromEnvironment: Option[String] =
    Utils.RuntimeDetector.detectViaClassLoader().orElse(Utils.RuntimeDetector.detectViaThreadNames())

  /** First 16 hex chars of SHA-256; the raw value never leaves the driver. */
  private[spark] def sha256Hex16(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(b => f"$b%02x").mkString.take(16)

  /** Truncated random table UUID: unique enough to count distinct tables, too short to join back. */
  private[spark] def tableId(tableUuid: String): Option[String] =
    Option(tableUuid).map(_.trim.toLowerCase)
      .filter(uuid => uuid.nonEmpty && uuid != ZERO_UUID)
      .map(_.replace("-", "").take(16))

  /** Derived flag only; the hostname itself never leaves the driver. */
  private[spark] def deployment(host: String): String =
    if (Option(host).exists(_.toLowerCase.endsWith(CLOUD_HOST_SUFFIX))) "cloud" else "self_managed"
}
