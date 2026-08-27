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
  format: String,
  engine: String
) {
  import UserAnalyticsEvent._

  // per event, not cached: only the reporting thread's stack shows the platform driving the job
  private val runtimeFromStack: Option[String] = Utils.RuntimeDetector.detectViaStackTrace()

  // also caller-stack-sensitive: the test framework's frames live on the reporting thread's stack
  private val isTestRun: Boolean = detectTestRun()

  def toParams: Seq[(String, String)] =
    Seq(
      "event" -> event,
      "version" -> connectorVersion,
      "spark_version" -> sparkVersion,
      "os" -> sys.props.getOrElse("os.name", "unknown"),
      "format" -> format,
      "engine" -> engine
    ) ++
      appNameHash.map("app_name_hash" -> _) ++
      tableId.map("table_id" -> _) ++
      (runtimeFromStack orElse runtimeFromEnvironment).map(r => "runtime" -> r.toLowerCase) ++
      (if (isTestRun) Some("test" -> "true") else None)
}

object UserAnalyticsEvent {

  val EVENT_READ: String = "read"
  val EVENT_WRITE: String = "write"

  // tables in legacy `Ordinary` databases carry the zero UUID, which identifies nothing
  private val ZERO_UUID = "00000000-0000-0000-0000-000000000000"

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

  // deterministic markers set by test-runner JVMs; presence is the signal, values are irrelevant
  private val TEST_MARKER_PROPS = Seq("org.gradle.test.worker", "surefire.test.class.path", Utils.IS_TESTING)

  // main classes of forked test-runner JVMs, matched against the start of `sun.java.command`
  private val TEST_RUNNER_MAINS = Seq(
    "worker.org.gradle.process.internal.worker.GradleWorkerMain",
    "org.apache.maven.surefire.booter.ForkedBooter",
    "org.scalatest.tools.Runner",
    "org.junit.platform.console.ConsoleLauncher",
    "sbt.ForkMain"
  )

  // package prefixes only, never bare substrings: these cannot appear in a production stack
  private val TEST_STACK_PREFIXES = Seq("org.scalatest.", "org.junit.", "munit.", "org.testng.")

  // a test-scope library in practice, so its mere presence marks an integration-test JVM; cached
  // because the classpath cannot change, unlike the per-event thread-sensitive signals
  private lazy val testcontainersOnClasspath: Boolean =
    try {
      Class.forName("org.testcontainers.containers.GenericContainer", false, getClass.getClassLoader)
      true
    } catch {
      case _: Throwable => false
    }

  /** True when this JVM carries a test-runner marker or a test framework is on the caller's stack. */
  private def detectTestRun(): Boolean =
    TEST_MARKER_PROPS.exists(key => System.getProperty(key) != null) ||
      sys.env.contains("SPARK_TESTING") ||
      Option(System.getProperty("sun.java.command")).exists(cmd => TEST_RUNNER_MAINS.exists(cmd.startsWith)) ||
      testcontainersOnClasspath ||
      Thread.currentThread().getStackTrace.exists(frame => TEST_STACK_PREFIXES.exists(frame.getClassName.startsWith))

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
}
