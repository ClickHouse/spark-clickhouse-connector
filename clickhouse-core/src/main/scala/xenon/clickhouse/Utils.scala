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

package xenon.clickhouse

import org.apache.commons.lang3.time.FastDateFormat

import java.io.{File, InputStream}
import java.math.{MathContext, RoundingMode}
import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.locks.LockSupport
import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object Utils extends Logging {

  @transient lazy val dateFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  @transient lazy val dateTimeFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  @transient lazy val legacyDateFmt: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
  @transient lazy val legacyDateTimeFmt: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def defaultClassLoader: ClassLoader =
    Try(Thread.currentThread.getContextClassLoader) // fail if cannot access thread context ClassLoader
      .orElse(Try(getClass.getClassLoader)) // fail indicates the bootstrap ClassLoader
      .orElse(Try(ClassLoader.getSystemClassLoader)) // fail if cannot access system ClassLoader
      .get

  def classpathResource(name: String): URI = defaultClassLoader.getResource(name).toURI

  def classpathResourceAsStream(name: String): InputStream = defaultClassLoader.getResourceAsStream(name)

  def getCodeSourceLocation(clazz: Class[_]): String = {
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
  }

  @transient lazy val tmpDirPath: Path = Files.createTempDirectory("classpath_res_")

  def copyFileFromClasspath(name: String): File = {
    val copyPath = tmpDirPath.resolve(name)
    Files.createDirectories(copyPath.getParent)
    tryWithResource(classpathResourceAsStream(name)) { input =>
      Files.copy(input, copyPath, StandardCopyOption.REPLACE_EXISTING)
    }
    copyPath.toFile
  }

  def load(key: String, defValue: String = ""): String = sys.props.getOrElse(key, sys.env.getOrElse(key, defValue))

  def stripSingleQuote(maybeQuoted: String): String = {
    var start = 0
    var until = maybeQuoted.length
    if (maybeQuoted.startsWith("'")) start = 1
    if (maybeQuoted.endsWith("'") && !maybeQuoted.endsWith("\\'")) until = until - 1
    if (start > until) until = start
    maybeQuoted.substring(start, until)
  }

  def wrapBackQuote(identifier: String): String = {
    val sb = new mutable.StringBuilder(identifier.length + 2)
    if (!identifier.startsWith("`")) sb.append('`')
    sb.append(identifier)
    if (identifier == "`" || !identifier.endsWith("`") || identifier.endsWith("\\`")) sb.append('`')
    sb.mkString
  }

  @tailrec
  def retry[R, T <: Throwable: ClassTag](retryTimes: Int, interval: Duration)(f: => R): Try[R] = {
    assert(retryTimes >= 0)
    val result = Try(f)
    result match {
      case Success(result) => Success(result)
      case Failure(exception: T) if retryTimes > 0 =>
        log.warn(s"Execution failed caused by: ", exception)
        log.warn(s"$retryTimes times retry remaining, the next will be in ${interval.toMillis}ms")
        LockSupport.parkNanos(interval.toNanos)
        retry(retryTimes - 1, interval)(f)
      case Failure(exception) => Failure(exception)
    }
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    val PiB = 1L << 50
    val TiB = 1L << 40
    val GiB = 1L << 30
    val MiB = 1L << 20
    val KiB = 1L << 10

    if (size >= BigInt(1L << 11) * EiB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EiB) {
          (BigDecimal(size) / EiB, "EiB")
        } else if (size >= 2 * PiB) {
          (BigDecimal(size) / PiB, "PiB")
        } else if (size >= 2 * TiB) {
          (BigDecimal(size) / TiB, "TiB")
        } else if (size >= 2 * GiB) {
          (BigDecimal(size) / GiB, "GiB")
        } else if (size >= 2 * MiB) {
          (BigDecimal(size) / MiB, "MiB")
        } else if (size >= 2 * KiB) {
          (BigDecimal(size) / KiB, "KiB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute
    val locale = Locale.US

    ms match {
      case t if t < second =>
        "%d ms".formatLocal(locale, t)
      case t if t < minute =>
        "%.1f s".formatLocal(locale, t.toFloat / second)
      case t if t < hour =>
        "%.1f m".formatLocal(locale, t.toFloat / minute)
      case t =>
        "%.2f h".formatLocal(locale, t.toFloat / hour)
    }
  }

  def tryWithResource[R <: AutoCloseable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource)
    finally resource.close()
  }

  /** Records the duration of running `body`. */
  def timeTakenMs[T](body: => T): (T, Long) = {
    val startTime = System.nanoTime()
    val result = body
    val endTime = System.nanoTime()
    (result, math.max(NANOSECONDS.toMillis(endTime - startTime), 0))
  }

  val IS_TESTING = "spark.testing"
  val PREFIX = "SPARK_ON_CLICKHOUSE"

  def setTesting(): Unit = System.setProperty(IS_TESTING, "true")

  def isTesting: Boolean = System.getProperty(IS_TESTING) == "true"
}
