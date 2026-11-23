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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.sql.{Date, Timestamp}
import java.time.Instant
import java.util.TimeZone

class SQLHelperSuite extends AnyFunSuite with SQLHelper {

  // A JVM default zone that differs from the catalog zone, as in issue #607.
  private def withJvmZone[T](zone: String)(body: => T): T = {
    val original = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(zone))
    try body
    finally TimeZone.setDefault(original)
  }

  private def compile(f: Filter): Option[String] = compileFilter(f)

  test("timestamp comparisons are UTC DateTime64 literals whatever the JVM zone") {
    // Spark's literal '2024-01-05 00:00:00' in America/Los_Angeles
    val instant = Instant.parse("2024-01-05T08:00:00Z")
    val expected = Some("`ts` >= toDateTime64('2024-01-05 08:00:00', 6, 'UTC')")
    Seq("UTC", "America/Los_Angeles", "Asia/Kolkata", "Asia/Tokyo").foreach { zone =>
      withJvmZone(zone) {
        assert(compile(GreaterThanOrEqual("ts", Timestamp.from(instant))) === expected, zone)
        assert(compile(GreaterThanOrEqual("ts", instant)) === expected, zone)
      }
    }
  }

  test("a scan that mixes in SQLHelper still serializes after compiling a timestamp") {
    // Scans such as ClickHouseBatchScan mix this trait in and are serialized to executors after the
    // driver has compiled the pushed filters, which initializes the (non-serializable) formatter.
    val helper = new SQLHelper with Serializable {}
    assert(helper.compileFilter(GreaterThan("ts", Instant.parse("2024-01-05T08:00:00Z"))).isDefined)
    val out = new ObjectOutputStream(new ByteArrayOutputStream())
    try out.writeObject(helper)
    finally out.close()
  }

  test("Timestamp and Instant literals render the same") {
    withJvmZone("America/Los_Angeles") {
      Seq("2024-01-05T08:00:00Z", "2024-01-05T08:00:00.5Z", "1969-12-31T23:59:59.999999Z").foreach { text =>
        val instant = Instant.parse(text)
        assert(compileValue(Timestamp.from(instant)) === compileValue(instant), text)
      }
    }
  }

  test("sub-second and pre-1970 comparisons keep their value") {
    val subSecond = Instant.parse("2024-01-05T08:00:00.123456Z")
    assert(compile(GreaterThan("ts", subSecond)) ===
      Some("`ts` > toDateTime64('2024-01-05 08:00:00.123456', 6, 'UTC')"))
    val pre1970 = Instant.parse("1969-07-20T20:17:40Z")
    assert(compile(LessThan("ts", Timestamp.from(pre1970))) ===
      Some("`ts` < toDateTime64('1969-07-20 20:17:40', 6, 'UTC')"))
  }

  test("IN with whole-second timestamps is pushed as UTC DateTime literals") {
    withJvmZone("America/Los_Angeles") {
      val values: Array[Any] = Array(
        Timestamp.from(Instant.parse("2024-01-05T08:00:00Z")),
        Instant.parse("2024-01-05T09:00:00Z")
      )
      assert(compile(In("ts", values)) === Some(
        "`ts` IN (toDateTime('2024-01-05 08:00:00', 'UTC'),toDateTime('2024-01-05 09:00:00', 'UTC'))"
      ))
    }
  }

  test("IN with a timestamp a DateTime cannot hold is not pushed") {
    Seq("2024-01-05T09:00:00.5Z", "1969-12-31T23:59:59Z", "2106-02-07T06:28:16Z").foreach { text =>
      val values: Array[Any] = Array(Timestamp.from(Instant.parse("2024-01-05T08:00:00Z")), Instant.parse(text))
      assert(compile(In("ts", values)) === None, text)
      assert(compile(Not(In("ts", values))) === None, text)
      assert(compile(And(In("ts", values), IsNotNull("ts"))) === None, text)
    }
    val bounds: Array[Any] = Array(Instant.EPOCH, Instant.parse("2106-02-07T06:28:15Z"))
    assert(compile(In("ts", bounds)).isDefined)
  }

  test("IN without timestamps is unchanged") {
    assert(compile(In("id", Array[Any](1, 2, 3))) === Some("`id` IN (1,2,3)"))
    assert(compile(In("s", Array[Any]("a", "b'c"))) === Some("`s` IN ('a','b''c')"))
  }

  test("Date literal stays a calendar date") {
    withJvmZone("America/Los_Angeles") {
      assert(compile(EqualTo("d", Date.valueOf("2024-01-05"))) === Some("`d` = '2024-01-05'"))
    }
  }
}
