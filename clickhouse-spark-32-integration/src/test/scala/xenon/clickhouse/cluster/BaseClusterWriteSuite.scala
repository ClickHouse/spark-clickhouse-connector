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

package xenon.clickhouse.cluster

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.QueryTest._
import org.apache.spark.sql.Row
import xenon.clickhouse.{BaseSparkSuite, Logging}
import xenon.clickhouse.base.ClickHouseClusterSuiteMixIn

abstract class BaseClusterWriteSuite extends BaseSparkSuite
    with ClickHouseClusterSuiteMixIn
    with SparkClickHouseClusterSuiteMixin
    with Logging {

  import spark.implicits._

  test("clickhouse write cluster") {
    val cluster = "single_replica"
    val db = "db_w"
    val tbl_dist = "t_dist"
    val tbl_local = s"${tbl_dist}_local"

    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db ON CLUSTER $cluster")

      spark.sql(
        s"""CREATE TABLE $db.$tbl_local (
           |  create_time TIMESTAMP NOT NULL,
           |  y           INT       NOT NULL COMMENT 'shard key',
           |  m           INT       NOT NULL COMMENT 'part key',
           |  id          BIGINT    NOT NULL COMMENT 'sort key',
           |  value       STRING
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (
           |  cluster = '$cluster',
           |  engine = 'MergeTree()',
           |  order_by = '(id)',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )

      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl_dist ON CLUSTER $cluster
           |AS $db.$tbl_local
           |ENGINE = Distributed($cluster, '$db', '$tbl_local', y)
           |""".stripMargin
      )

      val tblSchema = spark.table(s"$db.$tbl_dist").schema
      assert(tblSchema == StructType(
        StructField("create_time", DataTypes.TimestampType, nullable = false) ::
          StructField("y", DataTypes.IntegerType, nullable = false) ::
          StructField("m", DataTypes.IntegerType, nullable = false) ::
          StructField("id", DataTypes.LongType, nullable = false) ::
          StructField("value", DataTypes.StringType, nullable = true) :: Nil
      ))

      val dataDF = spark.createDataFrame(Seq(
        ("2021-01-01 10:10:10", 1L, "1"),
        ("2022-02-02 10:10:10", 2L, "2"),
        ("2023-03-03 10:10:10", 3L, "3"),
        ("2024-04-04 10:10:10", 4L, "4")
      )).toDF("create_time", "id", "value")
        .withColumn("create_time", to_timestamp($"create_time"))
        .withColumn("y", year($"create_time"))
        .withColumn("m", month($"create_time"))
        .select($"create_time", $"y", $"m", $"id", $"value")

      val dataDFWithExactlySchema = spark.createDataFrame(dataDF.rdd, tblSchema)

      dataDFWithExactlySchema
        .writeTo(s"$db.$tbl_dist")
        .append

      checkAnswer(
        spark.table(s"$db.$tbl_dist"),
        Seq(
          Row(Timestamp.valueOf("2021-01-01 10:10:10"), 2021, 1, 1L, "1"),
          Row(Timestamp.valueOf("2022-02-02 10:10:10"), 2022, 2, 2L, "2"),
          Row(Timestamp.valueOf("2023-03-03 10:10:10"), 2023, 3, 3L, "3"),
          Row(Timestamp.valueOf("2024-04-04 10:10:10"), 2024, 4, 4L, "4")
        )
      )

      checkAnswer(
        spark.table(s"`clickhouse-s1r1`.$db.$tbl_local"),
        Row(Timestamp.valueOf("2024-04-04 10:10:10"), 2024, 4, 4L, "4") :: Nil
      )
      checkAnswer(
        spark.table(s"`clickhouse-s1r2`.$db.$tbl_local"),
        Row(Timestamp.valueOf("2021-01-01 10:10:10"), 2021, 1, 1L, "1") :: Nil
      )
      checkAnswer(
        spark.table(s"`clickhouse-s2r1`.$db.$tbl_local"),
        Row(Timestamp.valueOf("2022-02-02 10:10:10"), 2022, 2, 2L, "2") :: Nil
      )
      checkAnswer(
        spark.table(s"`clickhouse-s2r2`.$db.$tbl_local"),
        Row(Timestamp.valueOf("2023-03-03 10:10:10"), 2023, 3, 3L, "3") :: Nil
      )

      // infiniteLoop()

    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_dist ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_local ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db ON CLUSTER $cluster")
    }
  }
}
