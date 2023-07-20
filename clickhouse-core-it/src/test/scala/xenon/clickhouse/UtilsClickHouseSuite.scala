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

import xenon.clickhouse.base.ClickHouseSingleMixIn

import java.time.{LocalDateTime, ZoneId}

class UtilsClickHouseSuite extends ClickHouseSingleMixIn with Logging {

  test("parse date with nano seconds") {
    withNodeClient() { client =>
      val tz = ZoneId.systemDefault()
      val sql = s"SELECT toDateTime64('2023-03-29 15:25:25.977654', 3, '$tz')"
      val output = client.syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(sql)
      assert(output.rows === 1L)
      val row = output.records.head
      assert(row.length === 1L)
      val actual = LocalDateTime.parse(row.head.asText, Utils.dateTimeFmt)
      val expected = LocalDateTime.of(2023, 3, 29, 15, 25, 25, 977000000)
      assert(actual === expected)
    }
  }
}
