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

package xenon.clickhouse.single

import java.lang.{Long => JLong}

import xenon.clickhouse.{BaseSparkSuite, ClickHouseUDF, Logging}
import xenon.clickhouse.JsonProtocol.om
import xenon.clickhouse.base.ClickHouseSingleSuiteMixIn

class ClickHouseUDFSuite extends BaseSparkSuite
    with ClickHouseSingleSuiteMixIn
    with SparkClickHouseSingleSuiteMixin
    with Logging {

  override def beforeAll: Unit =
    ClickHouseUDF.register

  test("UDF ck_xx_hash64") {
    def assertHashFuncEquals(stringVal: String): Unit = {
      val sparkResult = spark.sql(
        s"""SELECT
           |  ck_xx_hash64('$stringVal')          AS hash_value,
           |  ck_xx_hash64_shard(4, '$stringVal') AS shard_num
           |""".stripMargin
      ).collect
      assert(sparkResult.length == 1)
      val sparkHashVal = sparkResult.head.getAs[Long]("hash_value")
      val sparkShardNum = sparkResult.head.getAs[Long]("shard_num")

      val clickhouseResultJsonStr = runClickHouseSQL(
        s"""SELECT
           |  xxHash64('$stringVal')     AS hash_value,
           |  xxHash64('$stringVal') % 4 AS shard_num
           |""".stripMargin
      ).head.getString(0)
      val clickhouseResultJson = om.readTree(clickhouseResultJsonStr)
      val clickhouseHashVal = JLong.parseUnsignedLong(clickhouseResultJson.get("hash_value").asText)
      val clickhouseShardNum = JLong.parseUnsignedLong(clickhouseResultJson.get("shard_num").asText)

      assert(sparkHashVal == clickhouseHashVal)
      assert(sparkShardNum == clickhouseShardNum)
    }
    (
      "spark-clickhouse-connector" ::
        "Spark on ClickHouse" ::
        "Apache Spark" ::
        "ClickHouse" ::
        "Yandex" ::
        "热爱" ::
        "🇨🇳" ::
        Nil
    ).foreach(assertHashFuncEquals)
  }
}
