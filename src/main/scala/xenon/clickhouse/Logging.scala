package xenon.clickhouse

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  @transient lazy val log: Logger = LoggerFactory.getLogger(logName)

  protected def logName: String = this.getClass.getName.stripSuffix("$")
}
