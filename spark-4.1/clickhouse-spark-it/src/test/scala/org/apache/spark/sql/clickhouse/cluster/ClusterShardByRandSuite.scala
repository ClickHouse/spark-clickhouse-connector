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

import org.apache.spark.sql.Row

class ClusterShardByRandSuite extends SparkClickHouseClusterTest {

  test("shard by rand()") {
    val cluster = "single_replica"
    val db = "db_rand_shard"
    val tbl_dist = "tbl_rand_shard"
    val tbl_local = s"${tbl_dist}_local"

    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db ON CLUSTER $cluster")

      spark.sql(
        s"""CREATE TABLE $db.$tbl_local (
           |  create_time TIMESTAMP NOT NULL,
           |  value       STRING
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
           |ENGINE = Distributed($cluster, '$db', '$tbl_local', rand())
           |""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO `$db`.`$tbl_dist`
           |VALUES
           |  (timestamp'2021-01-01 10:10:10', '1'),
           |  (timestamp'2022-02-02 10:10:10', '2'),
           |  (timestamp'2023-03-03 10:10:10', '3'),
           |  (timestamp'2024-04-04 10:10:10', '4') AS tab(create_time, value)
           |""".stripMargin
      )
      checkAnswer(
        spark.table(s"$db.$tbl_dist").select("value").orderBy("create_time"),
        Seq(Row("1"), Row("2"), Row("3"), Row("4"))
      )
    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_dist ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_local ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db ON CLUSTER $cluster")
    }
  }
}
