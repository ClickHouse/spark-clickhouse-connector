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
import org.apache.spark.sql.clickhouse.SparkTest
import org.apache.spark.sql.functions.{month, year}
import com.clickhouse.spark.base.ClickHouseClusterMixIn

trait SparkClickHouseClusterTest extends SparkTest with ClickHouseClusterMixIn {

  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[4]")
    .setAppName("spark-clickhouse-cluster-ut")
    .set("spark.sql.shuffle.partitions", "4")
    // catalog
    .set("spark.sql.defaultCatalog", "clickhouse_s1r1")
    .set("spark.sql.catalog.clickhouse_s1r1", "com.clickhouse.spark.ClickHouseCatalog")
    .set("spark.sql.catalog.clickhouse_s1r1.host", clickhouse_s1r1_host)
    .set("spark.sql.catalog.clickhouse_s1r1.http_port", clickhouse_s1r1_http_port.toString)
    .set("spark.sql.catalog.clickhouse_s1r1.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s1r1.user", "default")
    .set("spark.sql.catalog.clickhouse_s1r1.password", "")
    .set("spark.sql.catalog.clickhouse_s1r1.database", "default")
    .set("spark.sql.catalog.clickhouse_s1r1.option.custom_http_params", "async_insert=1,wait_for_async_insert=1")
    .set("spark.sql.catalog.clickhouse_s1r2", "com.clickhouse.spark.ClickHouseCatalog")
    .set("spark.sql.catalog.clickhouse_s1r2.host", clickhouse_s1r2_host)
    .set("spark.sql.catalog.clickhouse_s1r2.http_port", clickhouse_s1r2_http_port.toString)
    .set("spark.sql.catalog.clickhouse_s1r2.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s1r2.user", "default")
    .set("spark.sql.catalog.clickhouse_s1r2.password", "")
    .set("spark.sql.catalog.clickhouse_s1r2.database", "default")
    .set("spark.sql.catalog.clickhouse_s1r2.option.custom_http_params", "async_insert=1,wait_for_async_insert=1")
    .set("spark.sql.catalog.clickhouse_s2r1", "com.clickhouse.spark.ClickHouseCatalog")
    .set("spark.sql.catalog.clickhouse_s2r1.host", clickhouse_s2r1_host)
    .set("spark.sql.catalog.clickhouse_s2r1.http_port", clickhouse_s2r1_http_port.toString)
    .set("spark.sql.catalog.clickhouse_s2r1.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s2r1.user", "default")
    .set("spark.sql.catalog.clickhouse_s2r1.password", "")
    .set("spark.sql.catalog.clickhouse_s2r1.database", "default")
    .set("spark.sql.catalog.clickhouse_s2r1.option.custom_http_params", "async_insert=1,wait_for_async_insert=1")
    .set("spark.sql.catalog.clickhouse_s2r2", "com.clickhouse.spark.ClickHouseCatalog")
    .set("spark.sql.catalog.clickhouse_s2r2.host", clickhouse_s2r2_host)
    .set("spark.sql.catalog.clickhouse_s2r2.http_port", clickhouse_s2r2_http_port.toString)
    .set("spark.sql.catalog.clickhouse_s2r2.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s2r2.user", "default")
    .set("spark.sql.catalog.clickhouse_s2r2.password", "")
    .set("spark.sql.catalog.clickhouse_s2r2.database", "default")
    .set("spark.sql.catalog.clickhouse_s2r2.option.custom_http_params", "async_insert=1,wait_for_async_insert=1")
    // extended configurations
    .set("spark.clickhouse.write.batchSize", "2")
    .set("spark.clickhouse.write.maxRetry", "2")
    .set("spark.clickhouse.write.retryInterval", "1")
    .set("spark.clickhouse.write.retryableErrorCodes", "241")
    .set("spark.clickhouse.write.write.repartitionNum", "0")
    .set("spark.clickhouse.write.distributed.useClusterNodes", "true")
    .set("spark.clickhouse.read.distributed.useClusterNodes", "false")
    .set("spark.clickhouse.write.distributed.convertLocal", "false")
    .set("spark.clickhouse.read.distributed.convertLocal", "true")
    .set("spark.clickhouse.read.format", "binary")
    .set("spark.clickhouse.write.format", "arrow")

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouse_s1r1_host,
    "http_port" -> clickhouse_s1r1_http_port.toString,
    "protocol" -> "http",
    "user" -> "default",
    "password" -> "",
    "database" -> "default"
  )

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
          (timestamp("2021-01-01T10:10:10Z"), 1L, "1"),
          (timestamp("2022-02-02T10:10:10Z"), 2L, "2"),
          (timestamp("2023-03-03T10:10:10Z"), 3L, "3"),
          (timestamp("2024-04-04T10:10:10Z"), 4L, "4")
        )).toDF("create_time", "id", "value")
          .withColumn("y", year($"create_time"))
          .withColumn("m", month($"create_time"))
          .select($"create_time", $"y", $"m", $"id", $"value")

        spark.createDataFrame(dataDF.rdd, tblSchema)
          .writeTo(s"$db.$tbl_dist")
          .append
        Thread.sleep(2000)
      }
      f(cluster, db, tbl_dist, tbl_local)
    }
}
