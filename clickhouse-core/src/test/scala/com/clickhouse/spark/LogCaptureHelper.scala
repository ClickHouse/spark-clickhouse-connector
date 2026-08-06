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

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, Logger}

import scala.collection.mutable.ArrayBuffer

/** Captures log4j events, for suites that assert on logging behavior. */
trait LogCaptureHelper {

  private class CapturingAppender extends AppenderSkeleton {
    val events: ArrayBuffer[LoggingEvent] = ArrayBuffer.empty[LoggingEvent]
    override def append(event: LoggingEvent): Unit = events += event
    override def close(): Unit = ()
    override def requiresLayout(): Boolean = false
  }

  /** Returns the WARN messages the given loggers produced while running `f`. */
  def captureWarnings(loggerNames: String*)(f: => Unit): Seq[String] = {
    val loggers = loggerNames.map(Logger.getLogger)
    val appender = new CapturingAppender
    val previousLevels = loggers.map(l => l -> l.getLevel)
    loggers.foreach { l => l.setLevel(Level.WARN); l.addAppender(appender) }
    try f
    finally previousLevels.foreach { case (l, level) => l.removeAppender(appender); l.setLevel(level) }
    appender.events
      .filter(_.getLevel == Level.WARN)
      .map(_.getRenderedMessage)
      .toSeq
  }
}
