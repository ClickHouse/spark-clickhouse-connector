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

package org.apache.spark.sql.clickhouse.cluster

import org.apache.spark.sql.clickhouse.TestUtils.om
import xenon.clickhouse.func.{CompositeFunctionRegistry, DynamicFunctionRegistry, StaticFunctionRegistry}
import xenon.clickhouse.func.clickhouse.ClickHouseXxHash64Shard

import java.lang.{Long => JLong}

class ClickHouseClusterHashUDFSuite extends SparkClickHouseClusterTest {
  // only for query function names
  val dummyRegistry: CompositeFunctionRegistry = {
    val dynamicFunctionRegistry = new DynamicFunctionRegistry
    val xxHash64ShardFunc = new ClickHouseXxHash64Shard(Seq.empty)
    dynamicFunctionRegistry.register("ck_xx_hash64_shard", xxHash64ShardFunc) // for compatible
    dynamicFunctionRegistry.register("clickhouse_shard_xxHash64", xxHash64ShardFunc)
    new CompositeFunctionRegistry(Array(StaticFunctionRegistry, dynamicFunctionRegistry))
  }

  def runTest(funcSparkName: String, funcCkName: String, stringVal: String): Unit = {
    val sparkResult = spark.sql(
      s"""SELECT
         |  $funcSparkName($stringVal)                         AS hash_value
         |""".stripMargin
    ).collect
    assert(sparkResult.length == 1)
    val sparkHashVal = sparkResult.head.getAs[Long]("hash_value")

    val clickhouseResultJsonStr = runClickHouseSQL(
      s"""SELECT
         |  $funcCkName($stringVal)     AS hash_value
         |""".stripMargin
    ).head.getString(0)
    val clickhouseResultJson = om.readTree(clickhouseResultJsonStr)
    val clickhouseHashVal = JLong.parseUnsignedLong(clickhouseResultJson.get("hash_value").asText)
    assert(sparkHashVal == clickhouseHashVal)
  }

  Seq(
    "clickhouse_xxHash64",
    "clickhouse_murmurHash3_64",
    "clickhouse_murmurHash3_32",
    "clickhouse_murmurHash2_64",
    "clickhouse_murmurHash2_32"
  ).foreach { funcSparkName =>
    val funcCkName = dummyRegistry.getFuncMappingBySpark(funcSparkName)
    test(s"UDF $funcSparkName") {
      Seq("spark-clickhouse-connector", "Apache Spark", "ClickHouse", "Yandex", "热爱", "🇨🇳").foreach { rawStringVal =>
        val stringVal = s"\'$rawStringVal\'"
        runTest(funcSparkName, funcCkName, stringVal)
      }
    }
  }
}
