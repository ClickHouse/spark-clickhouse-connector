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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class ClusterShardByTransformSuite extends SparkClickHouseClusterTest {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.write.distributed.convertLocal", "true")

  def runTest(func_name: String, func_args: Array[String]): Unit = {
    val func_expr = s"$func_name(${func_args.mkString(",")})"
    val cluster = "single_replica"
    val db = s"db_${func_name}_shard_transform"
    val tbl_dist = s"tbl_${func_name}_shard"
    val tbl_local = s"${tbl_dist}_local"

    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db ON CLUSTER $cluster")

      spark.sql(
        s"""CREATE TABLE $db.$tbl_local (
           |  create_time TIMESTAMP NOT NULL,
           |  create_date DATE NOT NULL,
           |  value       STRING NOT NULL
           |) USING ClickHouse
           |TBLPROPERTIES (
           |  cluster = '$cluster',
           |  engine = 'MergeTree()',
           |  order_by = 'create_time'
           |)
           |""".stripMargin
      )

      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl_dist ON CLUSTER $cluster
           |AS $db.$tbl_local
           |ENGINE = Distributed($cluster, '$db', '$tbl_local', $func_expr)
           |""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO `$db`.`$tbl_dist`
           |VALUES
           |  (timestamp'2021-01-01 10:10:10', date'2021-01-01', '1'),
           |  (timestamp'2022-02-02 11:10:10', date'2022-02-02', '2'),
           |  (timestamp'2023-03-03 12:10:10', date'2023-03-03', '3'),
           |  (timestamp'2024-04-04 13:10:10', date'2024-04-04', '4')
           |  AS tab(create_time, create_date, value)
           |""".stripMargin
      )
      // check that data is indeed written
      checkAnswer(
        spark.table(s"$db.$tbl_dist").select("value").orderBy("create_time"),
        Seq(Row("1"), Row("2"), Row("3"), Row("4"))
      )

      // check same data is sharded in the same server comparing native sharding
      runClickHouseSQL(
        s"""INSERT INTO `$db`.`$tbl_dist`
           |VALUES
           |  (timestamp'2021-01-01 10:10:10', date'2021-01-01', '1'),
           |  (timestamp'2022-02-02 11:10:10', date'2022-02-02', '2'),
           |  (timestamp'2023-03-03 12:10:10', date'2023-03-03', '3'),
           |  (timestamp'2024-04-04 13:10:10', date'2024-04-04', '4')
           |""".stripMargin
      )
      checkAnswer(
        spark.table(s"$db.$tbl_local")
          .groupBy("value").count().filter("count != 2"),
        Seq.empty
      )

    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_dist ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_local ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db ON CLUSTER $cluster")
    }
  }

  Seq(
    // TODO timezone aware implicit cast requires SPARK-44180
    ("toYear", Array("create_date")),
    // ("toYear", Array("create_time")),
    ("toYYYYMM", Array("create_date")),
    // ("toYYYYMM", Array("create_time")),
    ("toYYYYMMDD", Array("create_date")),
    // ("toYYYYMMDD", Array("create_time")),
    ("toHour", Array("create_time")),
    ("xxHash64", Array("value")),
    ("murmurHash2_64", Array("value")),
    ("murmurHash2_32", Array("value")),
    ("murmurHash3_64", Array("value")),
    ("murmurHash3_32", Array("value")),
    ("cityHash64", Array("value")),
    ("modulo", Array("toYYYYMM(create_date)", "10"))
  ).foreach {
    case (func_name: String, func_args: Array[String]) =>
      test(s"shard by $func_name(${func_args.mkString(",")})")(runTest(func_name, func_args))
  }

}
