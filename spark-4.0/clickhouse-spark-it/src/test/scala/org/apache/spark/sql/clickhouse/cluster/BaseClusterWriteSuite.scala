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

import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.types._

abstract class BaseClusterWriteSuite extends SparkClickHouseClusterTest {

  private val WAIT_MS = 2000

  test("clickhouse write cluster") {
    withSimpleDistTable("single_replica", "db_w", "t_dist", true) { (_, db, tbl_dist, tbl_local) =>
      val tblSchema = spark.table(s"$db.$tbl_dist").schema
      assert(tblSchema == StructType(
        StructField("create_time", DataTypes.TimestampType, nullable = false) ::
          StructField("y", DataTypes.IntegerType, nullable = false) ::
          StructField("m", DataTypes.IntegerType, nullable = false) ::
          StructField("id", DataTypes.LongType, nullable = false) ::
          StructField("value", DataTypes.StringType, nullable = true) :: Nil
      ))

      checkAnswer(
        spark
          .table(s"$db.$tbl_dist")
          .select("create_time", "y", "m", "id", "value"),
        Seq(
          Row(timestamp("2021-01-01T10:10:10Z"), 2021, 1, 1L, "1"),
          Row(timestamp("2022-02-02T10:10:10Z"), 2022, 2, 2L, "2"),
          Row(timestamp("2023-03-03T10:10:10Z"), 2023, 3, 3L, "3"),
          Row(timestamp("2024-04-04T10:10:10Z"), 2024, 4, 4L, "4")
        )
      )

      checkAnswer(
        spark.table(s"clickhouse_s1r1.$db.$tbl_local"),
        Row(timestamp("2024-04-04T10:10:10Z"), 2024, 4, 4L, "4") :: Nil
      )
      checkAnswer(
        spark.table(s"clickhouse_s1r2.$db.$tbl_local"),
        Row(timestamp("2021-01-01T10:10:10Z"), 2021, 1, 1L, "1") :: Nil
      )
      checkAnswer(
        spark.table(s"clickhouse_s2r1.$db.$tbl_local"),
        Row(timestamp("2022-02-02T10:10:10Z"), 2022, 2, 2L, "2") :: Nil
      )
      checkAnswer(
        spark.table(s"clickhouse_s2r2.$db.$tbl_local"),
        Row(timestamp("2023-03-03T10:10:10Z"), 2023, 3, 3L, "3") :: Nil
      )
    }
  }
}

class ClusterNodesWriteSuite extends BaseClusterWriteSuite {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.write.write.repartitionNum", "0")
    .set("spark.clickhouse.write.distributed.useClusterNodes", "true")
    .set("spark.clickhouse.write.distributed.convertLocal", "false")
}

class ConvertDistToLocalWriteSuite extends BaseClusterWriteSuite {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.write.write.repartitionNum", "0")
    .set("spark.clickhouse.write.distributed.useClusterNodes", "true")
    .set("spark.clickhouse.write.distributed.convertLocal", "true")

  test(
    "write to distributed table with unsupported sharding key fails when convertLocal=true and ignoreUnsupportedTransform=true"
  ) {
    val db = "db_unsupported_sharding"
    val tbl_dist = "t_dist_unsupported"
    val tbl_local = "t_local_unsupported"

    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db` ON CLUSTER 'single_replica'")

      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS `$db`.`$tbl_local` ON CLUSTER 'single_replica' (
           |  `id` Int64,
           |  `date_col` Date
           |) ENGINE = MergeTree()
           |ORDER BY id
           |SETTINGS index_granularity = 8192
           |""".stripMargin
      )

      // toQuarter returns integer but is not supported by Spark, making it a good test case for unsupported sharding keys
      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS `$db`.`$tbl_dist` (
           |  `id` Int64,
           |  `date_col` Date
           |) ENGINE = Distributed('single_replica', '$db', '$tbl_local', toQuarter(date_col))
           |""".stripMargin
      )

      Thread.sleep(WAIT_MS)

      val table = spark.table(s"clickhouse_s1r1.$db.$tbl_dist")
      assert(table.schema.nonEmpty, "Table should be accessible")

      import org.apache.spark.sql.functions._
      val testData = spark.range(3)
        .toDF("id")
        .withColumn("date_col", date_add(to_date(lit("2024-01-01")), col("id").cast("int")))

      val cause = intercept[CHClientException] {
        testData.writeTo(s"clickhouse_s1r1.$db.$tbl_dist").append()
      }
      assert(cause.getMessage.contains("may cause data corruption"))
      assert(
        cause.getMessage.contains("spark.clickhouse.write.distributed.convertLocal.allowUnsupportedSharding=true")
      )
      assert(cause.getMessage.contains("toQuarter"))

      withSQLConf(
        WRITE_DISTRIBUTED_CONVERT_LOCAL_ALLOW_UNSUPPORTED_SHARDING.key -> "true"
      ) {
        testData.writeTo(s"clickhouse_s1r1.$db.$tbl_dist").append()
        Thread.sleep(WAIT_MS)
        val count = spark.sql(s"SELECT COUNT(*) FROM clickhouse_s1r1.$db.$tbl_dist").collect()(0).getLong(0)
        assert(count >= 3)
      }

      withSQLConf(
        IGNORE_UNSUPPORTED_TRANSFORM.key -> "false"
      ) {
        runClickHouseSQL(s"TRUNCATE TABLE IF EXISTS `$db`.`$tbl_dist`")
        Thread.sleep(WAIT_MS)

        val cause = intercept[Exception] {
          testData.writeTo(s"clickhouse_s1r1.$db.$tbl_dist").append()
        }
        assert(cause.getMessage.contains("Unsupported") || cause.isInstanceOf[CHClientException])
      }

    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl_dist` ON CLUSTER 'single_replica'")
      runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl_local` ON CLUSTER 'single_replica'")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db` ON CLUSTER 'single_replica'")
    }
  }

  test("write to distributed table with supported sharding key succeeds regardless of configs") {
    val db = "db_supported_sharding"
    val tbl_dist = "t_dist_supported"
    val tbl_local = "t_local_supported"

    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db` ON CLUSTER 'single_replica'")

      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS `$db`.`$tbl_local` ON CLUSTER 'single_replica' (
           |  `id` Int64,
           |  `value` String
           |) ENGINE = MergeTree()
           |ORDER BY id
           |SETTINGS index_granularity = 8192
           |""".stripMargin
      )

      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS `$db`.`$tbl_dist` (
           |  `id` Int64,
           |  `value` String
           |) ENGINE = Distributed('single_replica', '$db', '$tbl_local', id)
           |""".stripMargin
      )

      Thread.sleep(WAIT_MS)

      // Create test data
      import org.apache.spark.sql.functions._
      val testData = spark.range(3)
        .toDF("id")
        .withColumn("value", col("id").cast("string"))

      testData.writeTo(s"clickhouse_s1r1.$db.$tbl_dist").append()
      Thread.sleep(WAIT_MS)
      val count = spark.sql(s"SELECT COUNT(*) FROM clickhouse_s1r1.$db.$tbl_dist").collect()(0).getLong(0)
      assert(count >= 3)

    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl_dist` ON CLUSTER 'single_replica'")
      runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl_local` ON CLUSTER 'single_replica'")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db` ON CLUSTER 'single_replica'")
    }
  }
}
