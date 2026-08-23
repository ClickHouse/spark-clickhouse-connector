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

package com.clickhouse.spark

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util.concurrent.{LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.util.control.NonFatal

/**
 * Delivers [[ScarfTelemetryEvent]]s to ClickHouse's Scarf (https://about.scarf.sh) gateway:
 * gates on the user's opt-outs, then fires one asynchronous, best-effort GET per event.
 * Failures are swallowed and can never fail or slow down the job.
 *
 * Disabled by `spark.clickhouse.telemetry.enabled=false`, or by setting the `SCARF_NO_ANALYTICS`
 * or `DO_NOT_TRACK` environment variable to `true` or `1`.
 */
private[spark] object ScarfTelemetry extends Logging {

  private val DEFAULT_ENDPOINT = "https://clickhouse.gateway.scarf.sh/spark-connector"
  private val TIMEOUT_MS = 3000

  // single daemon thread; a full queue silently drops events so a slow or unreachable
  // endpoint never blocks the driver nor piles up work
  private lazy val executor: ThreadPoolExecutor = {
    val threadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, "clickhouse-scarf-telemetry")
        thread.setDaemon(true)
        thread
      }
    }
    val pool = new ThreadPoolExecutor(
      1,
      1,
      30L,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable](4),
      threadFactory,
      new ThreadPoolExecutor.DiscardPolicy
    )
    pool.allowCoreThreadTimeOut(true)
    pool
  }

  private[spark] def disabledByEnv(env: String => Option[String] = k => sys.env.get(k)): Boolean = {
    def truthy(key: String): Boolean = env(key).exists(v => v.equalsIgnoreCase("true") || v == "1")
    truthy("SCARF_NO_ANALYTICS") || truthy("DO_NOT_TRACK")
  }

  private[spark] def buildUrl(event: ScarfTelemetryEvent, endpoint: String): String =
    event.toQueryParams
      .map { case (key, value) => s"$key=${URLEncoder.encode(value, "UTF-8")}" }
      .mkString(s"$endpoint?", "&", "")

  /** Fire-and-forget; never throws. `event` is by-name so no metadata is built when disabled. */
  def reportJobRun(
    event: => ScarfTelemetryEvent,
    enabledByConf: Boolean,
    endpoint: String = DEFAULT_ENDPOINT,
    env: String => Option[String] = k => sys.env.get(k)
  ): Unit =
    try
      if (enabledByConf && !disabledByEnv(env)) {
        val url = buildUrl(event, endpoint)
        executor.execute(() => send(url))
      }
    catch {
      case NonFatal(e) => log.debug(s"Skipped Scarf telemetry event: ${e.getMessage}")
    }

  private def send(url: String): Unit =
    try {
      val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(TIMEOUT_MS)
      connection.setReadTimeout(TIMEOUT_MS)
      connection.setRequestMethod("GET")
      connection.setRequestProperty(
        "User-Agent",
        s"spark-clickhouse-connector/${ScarfTelemetryEvent.connectorVersion}"
      )
      try connection.getResponseCode
      finally connection.disconnect()
    } catch {
      case NonFatal(e) => log.debug(s"Failed to send Scarf telemetry event: ${e.getMessage}")
    }
}
