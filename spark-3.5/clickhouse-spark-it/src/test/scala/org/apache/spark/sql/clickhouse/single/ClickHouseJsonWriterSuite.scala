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

package org.apache.spark.sql.clickhouse.single

import com.clickhouse.spark.base.ClickHouseSingleMixIn
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ClickHouseSingleJsonWriterSuite extends ClickHouseJsonWriterSuite with ClickHouseSingleMixIn

abstract class ClickHouseJsonWriterSuite extends ClickHouseWriterTestBase {

  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.write.format", "json")
    .set("spark.clickhouse.read.format", "json")

  test("push down per-write settings") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    withTable("test_db", "test_write_settings", schema) { (actualDb, actualTbl) =>
      val token1 = java.util.UUID.randomUUID().toString
      val token2 = java.util.UUID.randomUUID().toString
      val comment1 = s"write_settings_$token1"
      val comment2 = s"write_settings_$token2"

      Seq((1, "one")).toDF("id", "name").coalesce(1).writeTo(s"$actualDb.$actualTbl")
        .option("clickhouse_setting_insert_deduplication_token", token1)
        .option("clickhouse_setting_log_comment", comment1)
        .append()

      Seq((2, "two")).toDF("id", "name").coalesce(1).writeTo(s"$actualDb.$actualTbl")
        .option("clickhouse_setting_insert_deduplication_token", token2)
        .option("clickhouse_setting_log_comment", comment2)
        .append()

      runClickHouseSQL(
        if (isCloud) "SYSTEM FLUSH LOGS ON CLUSTER 'default'"
        else "SYSTEM FLUSH LOGS"
      ).collect()

      val queryLogRef =
        if (isCloud) "clusterAllReplicas('default', system, query_log)"
        else "system.query_log"
      val settings = runClickHouseSQL(
        s"""SELECT log_comment, Settings['insert_deduplication_token']
           |FROM $queryLogRef
           |WHERE type = 'QueryFinish'
           |  AND log_comment IN ('$comment1', '$comment2')
           |ORDER BY event_time DESC
           |LIMIT 10""".stripMargin
      ).collect().map(_.getString(0))

      assert(
        settings.exists(row => row.contains(comment1) && row.contains(token1)),
        s"Expected token $token1 in query_log. Found: ${settings.toList}"
      )
      assert(
        settings.exists(row => row.contains(comment2) && row.contains(token2)),
        s"Expected token $token2 in query_log. Found: ${settings.toList}"
      )
    }
  }

}
