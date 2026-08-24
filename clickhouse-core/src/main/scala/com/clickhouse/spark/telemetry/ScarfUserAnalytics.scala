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

import com.clickhouse.spark.Logging

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util.concurrent.{LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}

/**
 * [[UserAnalytics]] backed by ClickHouse's Scarf (https://about.scarf.sh) gateway:
 * gates on the user's opt-outs, then fires one asynchronous, best-effort GET per event.
 * Failures are swallowed and can never fail or slow down the job.
 *
 * Disabled by `spark.clickhouse.sendAnonymousUsageStats=false`, or by setting the `SCARF_NO_ANALYTICS`
 * or `DO_NOT_TRACK` environment variable to `true` or `1`.
 */
private[spark] class ScarfUserAnalytics(
  endpoint: String = ScarfUserAnalytics.DEFAULT_ENDPOINT,
  env: String => Option[String] = key => sys.env.get(key)
) extends UserAnalytics with Logging {
  import ScarfUserAnalytics._

  override def reportJobRun(event: => UserAnalyticsEvent, enabledByConf: => Boolean): Unit =
    try
      if (enabledByConf && !disabledByEnv(env)) {
        val builtEvent = event
        executor.execute(() => send(builtEvent))
      }
    catch {
      // Throwable, not NonFatal: a LinkageError while building the event must not reach the caller
      case e: Throwable => log.debug(s"Skipped Scarf usage analytics event: $e")
    }

  // builds the URL here too: its params include the runtime detection, which stays off the caller thread
  private def send(event: UserAnalyticsEvent): Unit =
    try {
      val connection = new URL(buildUrl(event, endpoint)).openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(TIMEOUT_MS)
      connection.setReadTimeout(TIMEOUT_MS)
      connection.setRequestMethod("GET")
      connection.setRequestProperty(
        "User-Agent",
        s"spark-clickhouse-connector/${UserAnalyticsEvent.connectorVersion}"
      )
      try connection.getResponseCode
      finally connection.disconnect()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        log.debug(s"Failed to send Scarf usage analytics event: $e")
      case e: Throwable => log.debug(s"Failed to send Scarf usage analytics event: $e")
    }
}

private[spark] object ScarfUserAnalytics {

  private val DEFAULT_ENDPOINT = "https://clickhouse.gateway.scarf.sh/spark-connector"
  private val TIMEOUT_MS = 3000

  // one JVM-wide FIFO daemon thread shared by all instances; a full queue silently drops
  // events so a slow or unreachable endpoint never blocks the driver nor piles up work
  private lazy val executor: ThreadPoolExecutor = {
    val threadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, "clickhouse-scarf-analytics")
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

  private[spark] def disabledByEnv(env: String => Option[String]): Boolean = {
    def truthy(key: String): Boolean = env(key).exists(v => v.equalsIgnoreCase("true") || v == "1")
    truthy("SCARF_NO_ANALYTICS") || truthy("DO_NOT_TRACK")
  }

  private[spark] def buildUrl(event: UserAnalyticsEvent, endpoint: String): String =
    event.toParams
      .map { case (key, value) => s"$key=${URLEncoder.encode(value, "UTF-8")}" }
      .mkString(s"$endpoint?", "&", "")
}
