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

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.QueryTest._
import org.apache.spark.sql.Row
import xenon.clickhouse.{BaseSparkSuite, Logging}
import xenon.clickhouse.base.ClickHouseSingleSuiteMixIn

class ClickHouseSingleSuite extends BaseSparkSuite
    with ClickHouseSingleSuiteMixIn
    with SparkClickHouseSingleSuiteMixin
    with Logging {

  import spark.implicits._

  test("clickhouse command runner") {
    checkAnswer(
      runClickHouseSQL("SELECT visibleWidth(NULL)"),
      Row("""{"visibleWidth(NULL)":"4"}""") :: Nil
    )
  }

  test("clickhouse catalog") {
    try {
      spark.sql("CREATE DATABASE db_t1")
      spark.sql("CREATE DATABASE db_t2")
      checkAnswer(
        spark.sql("SHOW DATABASES LIKE 'db_t*'"),
        Row("db_t1") :: Row("db_t2") :: Nil
      )
      spark.sql("USE system")
      checkAnswer(
        spark.sql("SELECT current_database()"),
        Row("system") :: Nil
      )
      assert(spark.sql("SHOW tables").where($"tableName" === "contributors").count === 1)
    } finally runClickHouseSQL(s"DROP DATABASE IF EXISTS db_t1")
  }

  ignore("clickhouse truncate table") {
    withClickHouseSingleIdTable("db_trunc", "tbl_trunc") { (db, tbl) =>
      spark.range(10).toDF("id").writeTo(s"$db.$tbl").append
      assert(spark.table(s"$db.$tbl").count == 10)
      spark.sql(s"TRUNCATE TABLE $db.$tbl")
      spark.sql(s"REFRESH TABLE $db.$tbl")
      assert(spark.table(s"$db.$tbl").count == 0)
    }
  }

  test("clickhouse write then read") {
    val db = "db_rw"
    val tbl = "tbl_rw"

    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db")

      // SPARK-33779: Spark 3.2 only support IdentityTransform
      spark.sql(
        s"""CREATE TABLE $db.$tbl (
           |  create_time TIMESTAMP NOT NULL,
           |  m           INT       NOT NULL COMMENT 'part key',
           |  id          BIGINT    NOT NULL COMMENT 'sort key',
           |  value       STRING
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = '(id)',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )

      val tblSchema = spark.table(s"$db.$tbl").schema
      assert(tblSchema == StructType(
        StructField("create_time", DataTypes.TimestampType, false) ::
          StructField("m", DataTypes.IntegerType, false) ::
          StructField("id", DataTypes.LongType, false) ::
          StructField("value", DataTypes.StringType, true) :: Nil
      ))

      val dataDF = spark.createDataFrame(Seq(
        ("2021-01-01 10:10:10", 1L, "1"),
        ("2022-02-02 10:10:10", 2L, "2")
      )).toDF("create_time", "id", "value")
        .withColumn("create_time", to_timestamp($"create_time"))
        .withColumn("m", month($"create_time"))
        .select($"create_time", $"m", $"id", $"value")

      val dataDFWithExactlySchema = spark.createDataFrame(dataDF.rdd, tblSchema)

      dataDFWithExactlySchema
        .writeTo(s"$db.$tbl")
        .append

      checkAnswer(
        spark.table(s"$db.$tbl"),
        Seq(
          Row(Timestamp.valueOf("2021-01-01 10:10:10"), 1, 1L, "1"),
          Row(Timestamp.valueOf("2022-02-02 10:10:10"), 2, 2L, "2")
        )
      )

      checkAnswer(
        spark.table(s"$db.$tbl").filter($"id" > 1),
        Row(Timestamp.valueOf("2022-02-02 10:10:10"), 2, 2L, "2") :: Nil
      )

      assert(spark.table(s"$db.$tbl").filter($"id" > 1).count === 1)

      // infiniteLoop()
    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db")
    }
  }
}
