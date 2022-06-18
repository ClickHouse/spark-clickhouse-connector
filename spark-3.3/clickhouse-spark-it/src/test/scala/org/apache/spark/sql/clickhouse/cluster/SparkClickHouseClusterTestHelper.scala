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

import org.apache.spark.sql.clickhouse.BaseSparkSuite
import org.apache.spark.sql.functions._

trait SparkClickHouseClusterTestHelper { self: BaseSparkSuite with SparkClickHouseClusterMixin =>
  import spark.implicits._

  def autoCleanupDistTable(
    cluster: String,
    db: String,
    tbl_dist: String
  )(f: (String, String, String, String) => Unit): Unit = {
    val tbl_local = s"${tbl_dist}_local"
    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db ON CLUSTER $cluster")
      f(cluster, db, tbl_dist, tbl_local)
    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_dist ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl_local ON CLUSTER $cluster")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db ON CLUSTER $cluster")
    }
  }

  def withSimpleDistTable(
    cluster: String,
    db: String,
    tbl_dist: String,
    writeData: Boolean = false
  )(f: (String, String, String, String) => Unit): Unit =
    autoCleanupDistTable(cluster, db, tbl_dist) { (cluster, db, tbl_dist, tbl_local) =>
      spark.sql(
        s"""CREATE TABLE $db.$tbl_dist (
           |  create_time TIMESTAMP NOT NULL,
           |  y           INT       NOT NULL COMMENT 'shard key',
           |  m           INT       NOT NULL COMMENT 'part key',
           |  id          BIGINT    NOT NULL COMMENT 'sort key',
           |  value       STRING
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (
           |  cluster = '$cluster',
           |  engine = 'Distributed',
           |  shard_by = 'y',
           |  local.engine = 'MergeTree()',
           |  local.database = '$db',
           |  local.table = '$tbl_local',
           |  local.order_by = 'id',
           |  local.settings.index_granularity = 8192
           |)
           |""".stripMargin
      )

      if (writeData) {
        val tblSchema = spark.table(s"$db.$tbl_dist").schema
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

        spark.createDataFrame(dataDF.rdd, tblSchema)
          .writeTo(s"$db.$tbl_dist")
          .append
      }
      f(cluster, db, tbl_dist, tbl_local)
    }
}
