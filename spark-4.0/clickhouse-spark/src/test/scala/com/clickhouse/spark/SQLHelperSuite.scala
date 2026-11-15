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

import org.apache.spark.sql.sources._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.{Instant, ZoneId}
import java.util.TimeZone

class SQLHelperSuite extends AnyFunSuite with SQLHelper {

  private val utc: ZoneId = ZoneId.of("UTC")

  // A JVM default zone that differs from the catalog zone, as in issue #607.
  private def withJvmZone[T](zone: String)(body: => T): T = {
    val original = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(zone))
    try body
    finally TimeZone.setDefault(original)
  }

  private def compile(f: Filter, tz: ZoneId = utc): Option[String] = compileFilter(f)(tz)

  test("Timestamp literal is rendered in the catalog zone, not the JVM zone") {
    withJvmZone("America/Los_Angeles") {
      // Spark's literal '2024-01-05 00:00:00' in America/Los_Angeles
      val ts = Timestamp.from(Instant.parse("2024-01-05T08:00:00Z"))
      assert(compile(GreaterThanOrEqual("ts", ts)) === Some("`ts` >= '2024-01-05 08:00:00'"))
      assert(compile(EqualTo("ts", ts), ZoneId.of("Asia/Kolkata")) === Some("`ts` = '2024-01-05 13:30:00'"))
    }
  }

  test("Instant literal is rendered in the catalog zone, not the JVM zone") {
    withJvmZone("America/Los_Angeles") {
      val instant = Instant.parse("2024-01-05T08:00:00Z")
      assert(compile(GreaterThanOrEqual("ts", instant)) === Some("`ts` >= '2024-01-05 08:00:00'"))
      assert(compile(EqualTo("ts", instant), ZoneId.of("Asia/Kolkata")) === Some("`ts` = '2024-01-05 13:30:00'"))
    }
  }

  test("Timestamp and Instant literals render the same") {
    withJvmZone("America/Los_Angeles") {
      Seq("2024-01-05T08:00:00Z", "2024-01-05T08:00:00.5Z", "1969-12-31T23:59:59.999999Z").foreach { text =>
        val instant = Instant.parse(text)
        assert(compileValue(Timestamp.from(instant))(utc) === compileValue(instant)(utc), text)
      }
    }
  }

  test("sub-second Timestamp and Instant literals keep their precision as a UTC DateTime64") {
    withJvmZone("America/Los_Angeles") {
      val instant = Instant.parse("2024-01-05T08:00:00.123456Z")
      val expected = Some("`ts` > toDateTime64('2024-01-05 08:00:00.123456', 6, 'UTC')")
      assert(compile(GreaterThan("ts", Timestamp.from(instant))) === expected)
      assert(compile(GreaterThan("ts", instant)) === expected)
      assert(compile(LessThan("ts", instant), ZoneId.of("Asia/Kolkata")) ===
        Some("`ts` < toDateTime64('2024-01-05 08:00:00.123456', 6, 'UTC')"))
    }
  }

  test("IN with whole-second timestamps is pushed in the catalog zone") {
    withJvmZone("America/Los_Angeles") {
      val values: Array[Any] = Array(
        Timestamp.from(Instant.parse("2024-01-05T08:00:00Z")),
        Instant.parse("2024-01-05T09:00:00Z")
      )
      assert(compile(In("ts", values)) === Some("`ts` IN ('2024-01-05 08:00:00','2024-01-05 09:00:00')"))
    }
  }

  test("IN with a sub-second timestamp is not pushed") {
    val values: Array[Any] = Array(
      Timestamp.from(Instant.parse("2024-01-05T08:00:00Z")),
      Timestamp.from(Instant.parse("2024-01-05T09:00:00.5Z"))
    )
    assert(compile(In("ts", values)) === None)
    assert(compile(Not(In("ts", values))) === None)
    assert(compile(And(In("ts", values), IsNotNull("ts"))) === None)
  }

  test("Date literal stays a calendar date") {
    withJvmZone("America/Los_Angeles") {
      assert(compile(EqualTo("d", Date.valueOf("2024-01-05"))) === Some("`d` = '2024-01-05'"))
    }
  }
}
