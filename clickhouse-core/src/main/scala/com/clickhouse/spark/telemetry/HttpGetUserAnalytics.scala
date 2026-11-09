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
import java.util.concurrent.{
  ConcurrentHashMap,
  LinkedBlockingQueue,
  RejectedExecutionException,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}

/**
 * [[UserAnalytics]] delivering events as query params on a plain HTTP GET: gates on the user's
 * opt-outs, then fires one asynchronous, best-effort request per event type and table, once per
 * JVM. Failures are swallowed and can never fail or slow down the job.
 *
 * Disabled by `spark.clickhouse.sendAnonymousUsageStats=false`, or by setting the `DO_NOT_TRACK`
 * environment variable to `true` or `1`.
 */
private[spark] class HttpGetUserAnalytics(
  endpoint: String = HttpGetUserAnalytics.resolveEndpoint(),
  env: String => Option[String] = key => sys.env.get(key),
  reportedKeys: HttpGetUserAnalytics.ReportedKeys = HttpGetUserAnalytics.alreadyReported
) extends UserAnalytics with Logging {
  import HttpGetUserAnalytics._

  override def reportJobRun(event: => UserAnalyticsEvent, enabledByConf: => Boolean): Unit =
    try
      if (enabledByConf && !disabledByEnv(env)) {
        val builtEvent = event
        val key = (builtEvent.event, builtEvent.tableId)
        if (reportedKeys.size < MAX_REPORTED_KEYS && reportedKeys.add(key)) {
          try executor.execute(() => send(builtEvent))
          catch {
            // dropped by the full queue: free the key so the table's next event can still be reported
            case e: RejectedExecutionException =>
              reportedKeys.remove(key)
              throw e
          }
        }
      }
    catch {
      // Throwable, not NonFatal: a LinkageError while building the event must not reach the caller
      case e: Throwable => log.debug(s"Skipped usage analytics event: $e")
    }

  // builds the URL here too: its costlier params, like the class loader probing, stay off the caller thread
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
        log.debug(s"Failed to send usage analytics event: $e")
      case e: Throwable => log.debug(s"Failed to send usage analytics event: $e")
    }
}

private[spark] object HttpGetUserAnalytics {

  private val DEFAULT_ENDPOINT = "https://telemetry.clickhouse.com/data"
  private val ENDPOINT_ENV = "CLICKHOUSE_TELEMETRY_ENDPOINT"
  private val TIMEOUT_MS = 3000

  /** (event type, table id) pairs already reported. */
  private[spark] type ReportedKeys = java.util.Set[(String, Option[String])]

  // bounds the set on drivers touching many tables; past it, new tables go unreported
  private[spark] val MAX_REPORTED_KEYS = 1000

  // JVM-wide like the sending thread, since every scan and write creates its own sink
  private val alreadyReported: ReportedKeys = newReportedKeys()

  private[spark] def newReportedKeys(): ReportedKeys = ConcurrentHashMap.newKeySet[(String, Option[String])]()

  /** Deliberately undocumented override, e.g. to point an end-to-end test at a local capture server. */
  private[spark] def resolveEndpoint(env: String => Option[String] = key => sys.env.get(key)): String =
    env(ENDPOINT_ENV).map(_.trim).filter(_.nonEmpty).getOrElse(DEFAULT_ENDPOINT)

  // one JVM-wide FIFO daemon thread shared by all instances; a full queue rejects events, which
  // `reportJobRun` drops, so a slow or unreachable endpoint never blocks the driver nor piles up work
  private lazy val executor: ThreadPoolExecutor = {
    val threadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, "clickhouse-usage-analytics")
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
      new ThreadPoolExecutor.AbortPolicy
    )
    pool.allowCoreThreadTimeOut(true)
    pool
  }

  private[spark] def disabledByEnv(env: String => Option[String]): Boolean =
    env("DO_NOT_TRACK").exists(v => v.equalsIgnoreCase("true") || v == "1")

  private[spark] def buildUrl(event: UserAnalyticsEvent, endpoint: String): String =
    event.toParams
      .map { case (key, value) => s"$key=${URLEncoder.encode(value, "UTF-8")}" }
      .mkString(s"$endpoint?", "&", "")
}
