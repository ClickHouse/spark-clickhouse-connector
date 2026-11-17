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
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader
import com.clickhouse.client.api.query.QuerySettings
import com.clickhouse.spark.format.StartOfDayDates.exactEpochDay
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayInputStream
import java.time.{LocalDate, ZoneId}
import java.util.TimeZone
import scala.collection.JavaConverters._
import scala.util.Try

class StartOfDayDatesSuite extends AnyFunSuite {

  // Date is UInt16 days; Date32 spans 1900-01-01 to 2299-12-31
  private val dateDays = 0 to 65535
  private val date32Days = LocalDate.of(1900, 1, 1).toEpochDay.toInt to LocalDate.of(2299, 12, 31).toEpochDay.toInt

  test("2011-12-30 in Pacific/Apia reads as 2011-12-31, like client-v2") {
    val apia = ZoneId.of("Pacific/Apia")
    val dates = new StartOfDayDates(apia)
    val day = LocalDate.of(2011, 12, 30).toEpochDay.toInt
    assert(dates.epochDay(day) === day + 1)
    assert(dates.epochDay(day + 1) === day + 1)
    assert(dates.epochDay(day - 1) === day - 1)
    // client-v2's own conversion
    val zdt = BinaryStreamReader.readDate32(
      new ByteArrayInputStream(RowBinaryTestData.int32LE(day).bytes),
      new Array[Byte](4),
      TimeZone.getTimeZone(apia)
    )
    assert(zdt.toLocalDate.toEpochDay === day + 1)
  }

  test("every Date and Date32 day matches client-v2 in zones that skipped, repeated or shifted days") {
    val zones = Seq(
      "Pacific/Apia",
      "Pacific/Fakaofo",
      "Pacific/Kwajalein",
      "Pacific/Kiritimati",
      "Pacific/Enderbury",
      "Asia/Manila",
      "America/Juneau",
      "America/Sao_Paulo",
      "America/Havana",
      "Asia/Tehran",
      "Asia/Gaza",
      "Africa/Casablanca",
      "Europe/Berlin",
      "UTC",
      "GMT+05:30"
    )
    for (zone <- zones.map(ZoneId.of)) {
      val dates = new StartOfDayDates(zone)
      val days = dateDays.iterator ++ date32Days.iterator ++ Iterator(Int.MinValue, -1, Int.MaxValue)
      val mismatches = days.filter(day => dates.epochDay(day) != exactEpochDay(day, zone)).take(5).toList
      assert(mismatches.isEmpty, s"$zone")
    }
  }

  test("days around every time zone transition match client-v2") {
    for (id <- ZoneId.getAvailableZoneIds.asScala.toSeq.sorted) {
      val zone = ZoneId.of(id)
      val dates = new StartOfDayDates(zone)
      val days = zone.getRules.getTransitions.asScala.flatMap { transition =>
        val day = transition.getDateTimeBefore.toLocalDate.toEpochDay.toInt
        day - 2 to day + 2
      }
      val mismatches = days.filter(day => dates.epochDay(day) != exactEpochDay(day, zone)).take(5).toList
      assert(mismatches.isEmpty, s"$id")
    }
  }

  test("client time zone: the server's unless use_server_time_zone is off, as client-v2 picks it") {
    def settings(options: (String, AnyRef)*): QuerySettings = {
      val s = new QuerySettings()
      options.foreach { case (k, v) => s.setOption(k, v) }
      s
    }
    val useServer = ClientConfigProperties.USE_SERVER_TIMEZONE.getKey
    val useTimeZone = ClientConfigProperties.USE_TIMEZONE.getKey
    val server = ClientConfigProperties.SERVER_TIMEZONE.getKey
    val apia = TimeZone.getTimeZone("Pacific/Apia")
    val tokyo = TimeZone.getTimeZone("Asia/Tokyo")

    val cases = Seq(
      settings(useServer -> java.lang.Boolean.TRUE, server -> apia, useTimeZone -> tokyo) -> Some(apia),
      settings(useServer -> java.lang.Boolean.FALSE, server -> apia, useTimeZone -> tokyo) -> Some(tokyo),
      settings(useServer -> java.lang.Boolean.TRUE) -> None,
      settings(useServer -> java.lang.Boolean.FALSE) -> None,
      settings(useServer -> "true", server -> apia) -> None,
      settings(useTimeZone -> tokyo) -> Some(tokyo),
      settings() -> None
    )
    cases.foreach { case (querySettings, expected) =>
      assert(StartOfDayDates.clientTimeZone(querySettings) === expected, querySettings.getAllSettings)
      // client-v2 fails to create its reader exactly when there is no time zone
      val clientFails = Try(new RowBinaryWithNamesAndTypesFormatReader(
        new ByteArrayInputStream(Array.emptyByteArray),
        querySettings,
        new BinaryStreamReader.DefaultByteBufferAllocator
      )).isFailure
      assert(clientFails === expected.isEmpty, querySettings.getAllSettings)
    }
    assert(StartOfDayDates.clientTimeZone(null).isEmpty)
  }
}
