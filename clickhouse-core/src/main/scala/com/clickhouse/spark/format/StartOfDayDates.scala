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

package com.clickhouse.spark.format

import com.clickhouse.client.api.ClientConfigProperties
import com.clickhouse.client.api.query.QuerySettings

import java.time.{LocalDate, ZoneId}
import java.util.{Arrays, Collections, TimeZone}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Epoch days of Date and Date32 values as client-v2 reads them: the start of the day in `zone`, then its local date.
 * That is the day itself, except in a zone that skipped the whole day (2011-12-30 in Pacific/Apia reads as 12-31).
 */
final class StartOfDayDates(zone: ZoneId) {
  import StartOfDayDates._

  private val rules = zone.getRules

  // Only a gap of a day or more moves a date; should a recurring rule have one, every day is converted exactly
  private val exactOnly = rules.getTransitionRules.asScala.exists { rule =>
    rule.getOffsetAfter.getTotalSeconds - rule.getOffsetBefore.getTotalSeconds >= SecondsPerDay
  }

  private val (shiftedDays, shiftedTo) = {
    val shifted = mutable.TreeMap.empty[Int, Int]
    rules.getTransitions.asScala.filter(_.isGap).foreach { gap =>
      val first = gap.getDateTimeBefore.toLocalDate.toEpochDay
      val last = gap.getDateTimeAfter.toLocalDate.toEpochDay
      (first to last).filter(day => day.isValidInt).map(_.toInt).foreach { day =>
        val to = exactEpochDay(day, zone)
        if (to != day) shifted(day) = to
      }
    }
    (shifted.keys.toArray, shifted.values.toArray)
  }

  def epochDay(day: Int): Int =
    if (exactOnly) exactEpochDay(day, zone)
    else if (shiftedDays.isEmpty || day < shiftedDays(0) || day > shiftedDays(shiftedDays.length - 1)) day
    else {
      val i = Arrays.binarySearch(shiftedDays, day)
      if (i >= 0) shiftedTo(i) else day
    }
}

object StartOfDayDates {
  private val SecondsPerDay = 24 * 60 * 60

  /** client-v2's `readDate` conversion, then `toLocalDate.toEpochDay` as `ClickHouseBinaryReader` does. */
  def exactEpochDay(day: Int, zone: ZoneId): Int =
    LocalDate.ofEpochDay(day).atStartOfDay(zone).withZoneSameInstant(zone).toLocalDate.toEpochDay.toInt

  /** Time zone client-v2's `AbstractBinaryFormatReader` uses for dates; None where it would throw instead. */
  def clientTimeZone(settings: QuerySettings): Option[TimeZone] = Try {
    val all =
      if (settings == null) Collections.emptyMap[String, AnyRef]() else settings.getAllSettings
    val useServerTimeZone = all.get(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey).asInstanceOf[java.lang.Boolean]
    if ((useServerTimeZone eq java.lang.Boolean.TRUE) && settings != null) settings.getServerTimeZone
    else all.get(ClientConfigProperties.USE_TIMEZONE.getKey).asInstanceOf[TimeZone]
  }.toOption.flatMap(Option(_))
}
