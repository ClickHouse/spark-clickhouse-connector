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

import com.clickhouse.spark.read.ScanJobDescription
import com.clickhouse.spark.write.WriteJobDescription
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession

import scala.util.Try

/** Extracts [[UserAnalyticsEvent]] metadata from Spark and the connector's job descriptions. */
object UserAnalyticsEvents {

  def readEvent(scanJob: ScanJobDescription): UserAnalyticsEvent =
    event(
      UserAnalyticsEvent.EVENT_READ,
      scanJob.node.host,
      scanJob.tableSpec.uuid,
      scanJob.tableSpec.engine,
      scanJob.readOptions.format,
      scanJob.readOptions.convertDistributedToLocal
    )

  def writeEvent(writeJob: WriteJobDescription): UserAnalyticsEvent =
    event(
      UserAnalyticsEvent.EVENT_WRITE,
      writeJob.node.host,
      writeJob.tableSpec.uuid,
      writeJob.tableSpec.engine,
      writeJob.writeOptions.format,
      writeJob.writeOptions.convertDistributedToLocal
    )

  private def event(
    eventType: String,
    host: String,
    tableUuid: String,
    engine: String,
    format: String,
    convertLocal: Boolean
  ): UserAnalyticsEvent =
    UserAnalyticsEvent(
      event = eventType,
      sparkVersion = SPARK_VERSION,
      appNameHash = Try(SparkSession.active.sparkContext.appName).toOption.map(UserAnalyticsEvent.sha256Hex16),
      tableId = UserAnalyticsEvent.tableId(tableUuid),
      deployment = UserAnalyticsEvent.deployment(host),
      format = format,
      engine = engine,
      convertLocal = convertLocal
    )
}
