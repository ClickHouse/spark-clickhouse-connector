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

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property

import scala.collection.mutable.ArrayBuffer

/** log4j2 counterpart of [[LogCaptureHelper]], for suites whose logging backend is log4j2. */
trait Log4j2CaptureHelper {

  /** Runs `f` and returns its result with the WARN messages the given logger emitted while it ran. */
  def captureWarnings[A](loggerName: String)(f: => A): (A, Seq[String]) = {
    val warnings = ArrayBuffer.empty[String]
    val appender: AbstractAppender =
      new AbstractAppender("log-capture-helper", null, null, false, Property.EMPTY_ARRAY) {
        override def append(event: LogEvent): Unit =
          if (event.getLevel == Level.WARN && event.getLoggerName == loggerName)
            warnings += event.getMessage.getFormattedMessage
      }
    appender.start()
    // attach to the nearest configured LoggerConfig: Logger.addAppender would register a permanent
    // config for `loggerName` that black-holes its logging once the appender is removed
    val loggerConfig = LogManager.getLogger(loggerName).asInstanceOf[Logger]
      .getContext.getConfiguration.getLoggerConfig(loggerName)
    loggerConfig.addAppender(appender, Level.WARN, null)
    val result =
      try f
      finally {
        loggerConfig.removeAppender(appender.getName)
        appender.stop()
      }
    (result, warnings.toSeq)
  }
}
