package xenon.clickhouse

import java.net.URI
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.LockSupport

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.commons.lang3.time.FastDateFormat

object Utils extends Logging {

  @transient lazy val dateFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  @transient lazy val dateTimeFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  @transient lazy val legacyDateFmt: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
  @transient lazy val legacyDateTimeFmt: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  @transient lazy val om: ObjectMapper with ScalaObjectMapper = {
    val _om = new ObjectMapper() with ScalaObjectMapper
    _om.findAndRegisterModules()
    _om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _om
  }

  def defaultClassLoader: ClassLoader =
    Try(Thread.currentThread.getContextClassLoader) // fail if cannot access thread context ClassLoader
      .orElse(Try(getClass.getClassLoader)) // fail indicates the bootstrap ClassLoader
      .orElse(Try(ClassLoader.getSystemClassLoader)) // fail if cannot access system ClassLoader
      .get

  def classpathResource(name: String): URI = defaultClassLoader.getResource(name).toURI

  def load(key: String, defValue: String = ""): String = sys.props.getOrElse(key, sys.env.getOrElse(key, defValue))

  @tailrec
  def retry[R, T <: Throwable: ClassTag](retryTimes: Int, interval: Duration)(f: () => R): Try[R] = {
    assert(retryTimes >= 0)
    val clazz = implicitly[ClassTag[T]].runtimeClass
    Try(f()) match {
      case Success(result) => Success(result)
      case Failure(exception) if clazz.isInstance(exception) && retryTimes > 0 =>
        log.warn(s"execution failed cause by", exception)
        log.warn(s"$retryTimes times retry remaining, the next will be in ${interval.toMillis}ms")
        LockSupport.parkNanos(interval.toNanos)
        retry(retryTimes - 1, interval)(f)
      case Failure(exception) => Failure(exception)
    }
  }

  val PREFIX = "SPARK_ON_CLICKHOUSE"

  def setTesting(name: String = "ut"): Unit = sys.props += ((s"${PREFIX}_TESTING", name))

  def unsetTesting(): Unit = sys.props -= s"${PREFIX}_TESTING"

  def isTesting: Boolean = sys.env.contains(s"${PREFIX}_TESTING") || sys.props.contains(s"${PREFIX}_TESTING")
}
