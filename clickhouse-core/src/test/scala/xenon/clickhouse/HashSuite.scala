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
import xenon.clickhouse.client.NodeClient
import xenon.clickhouse.hash.CityHash64

import java.time.{LocalDateTime, ZoneId}

class HashSuite extends ClickHouseSingleMixIn with Logging {

  def testHash[T <: Array[Any]](
    client: NodeClient,
    fun: T => Long,
    testInput: T,
    clickhouseFunName: String,
    clickhouseInputExpr: String
  ): Unit = {
    val sql = s"SELECT toInt64($clickhouseFunName($clickhouseInputExpr))"
    val output = client.syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(sql)
    assert(output.rows === 1L)
    val row = output.records.head
    assert(row.length === 1L)
    try
      row.head.asText.toLong
    catch {
      case _: NumberFormatException =>
        fail(s"clickhouse function's return should be a long, but got ${row.head.asText}")
    }
    val actual = row.head.asText.toLong
    val expected = fun(testInput)
    assert(actual === expected)
  }

  test("CityHash64 Java implementation") {
    withNodeClient() { client =>
      val testElement: Array[Any] = Array(
        "spark-clickhouse-connector",
        "Apache Spark",
        "ClickHouse",
        "Yandex",
        "热爱",
        "🇨🇳",
        "This is a long test text. 在传统的行式数据库系统中，数据按如下顺序存储。🇨🇳" * 5
      )
      val testInputs = testElement.combinations(1) ++ testElement.combinations(2) ++ testElement.combinations(3)
      testInputs.foreach { testInput =>
        val clickhouseInputExpr = testInput.mkString("'", "', '", "'")
        testHash(client, x => CityHash64(x), testInput, "cityHash64", clickhouseInputExpr)
      }
    }
  }
}
