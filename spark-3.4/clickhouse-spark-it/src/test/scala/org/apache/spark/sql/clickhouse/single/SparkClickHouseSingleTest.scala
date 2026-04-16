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

import com.clickhouse.spark.base.{ClickHouseProvider, ClickHouseSingleMixIn, RetryUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.clickhouse.SparkTest
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll

import java.util.UUID
import scala.util.{Failure, Success, Try}

trait SparkClickHouseSingleTest extends SparkTest with ClickHouseProvider with BeforeAndAfterAll {

  import testImplicits._

  protected lazy val testDatabaseName: String = {
    val timestamp = System.currentTimeMillis()
    val uuidPrefix = UUID.randomUUID().toString.split("-").head
    s"test_db_${timestamp}_${uuidPrefix}"
  }

  protected def useSuiteLevelDatabase: Boolean = isCloud

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (useSuiteLevelDatabase) {
      createDatabaseWithRetry(testDatabaseName)
    }
  }

  override def afterAll(): Unit =
    try
      if (useSuiteLevelDatabase) {
        dropDatabaseWithRetry(testDatabaseName)
      }
    finally
      super.afterAll()

  override protected def createDatabaseWithRetry(db: String, maxRetries: Int = 5): Unit = {
    super.createDatabaseWithRetry(db, maxRetries)
    if (isCloud) waitForDatabaseVisible(db)
  }

  override protected def dropTableWithRetry(db: String, tbl: String, maxRetries: Int = 5): Unit = {
    super.dropTableWithRetry(db, tbl, maxRetries)
    if (isCloud) waitForTableGone(db, tbl)
  }

  private def waitForDatabaseVisible(db: String, timeoutMs: Long = 30000): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    var visible = false
    while (!visible && System.currentTimeMillis() < deadline) {
      visible = Try(
        spark.sql("SHOW DATABASES").collect().exists(_.getString(0) == db)
      ).getOrElse(false)
      if (!visible) Thread.sleep(1000)
    }
    if (!visible)
      throw new RuntimeException(s"Database $db not visible after ${timeoutMs}ms")
  }

  private def waitForTableGone(db: String, tbl: String, timeoutMs: Long = 15000): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    var gone = false
    while (!gone && System.currentTimeMillis() < deadline) {
      gone = Try(
        !spark.sql(s"SHOW TABLES IN $db").collect().exists(_.getString(1) == tbl)
      ).getOrElse(true)
      if (!gone) Thread.sleep(1000)
    }
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[2]")
    .setAppName("spark-clickhouse-single-ut")
    .set("spark.sql.shuffle.partitions", "2")
    // catalog
    .set("spark.sql.defaultCatalog", "clickhouse")
    .set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    .set("spark.sql.catalog.clickhouse.host", clickhouseHost)
    .set("spark.sql.catalog.clickhouse.http_port", clickhouseHttpPort.toString)
    .set("spark.sql.catalog.clickhouse.protocol", "http")
    .set("spark.sql.catalog.clickhouse.user", clickhouseUser)
    .set("spark.sql.catalog.clickhouse.password", clickhousePassword)
    .set("spark.sql.catalog.clickhouse.database", clickhouseDatabase)
    .set("spark.sql.catalog.clickhouse.option.clickhouse_setting_wait_for_async_insert", "1")
    .set("spark.sql.catalog.clickhouse.option.clickhouse_setting_async_insert_deduplicate", "0")
    .set("spark.sql.catalog.clickhouse.option.ssl", isSslEnabled.toString)
    // extended configurations
    .set("spark.clickhouse.write.batchSize", "2")
    .set("spark.clickhouse.write.maxRetry", "2")
    .set("spark.clickhouse.write.retryInterval", "1")
    .set("spark.clickhouse.write.retryableErrorCodes", "241")
    .set("spark.clickhouse.write.write.repartitionNum", "0")
    .set("spark.clickhouse.read.format", "json")
    .set("spark.clickhouse.write.format", "json")

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouseHost,
    "http_port" -> clickhouseHttpPort.toString,
    "protocol" -> "http",
    "user" -> clickhouseUser,
    "password" -> clickhousePassword,
    "database" -> clickhouseDatabase,
    "option.clickhouse_setting_wait_for_async_insert" -> "1",
    "option.clickhouse_setting_async_insert_deduplicate" -> "0",
    "option.ssl" -> isSslEnabled.toString
  )

  def withTable(
    db: String,
    tbl: String,
    schema: StructType,
    engine: String = "MergeTree()",
    sortKeys: Seq[String] = "id" :: Nil,
    partKeys: Seq[String] = Seq.empty
  )(f: (String, String) => Unit): Unit = {
    val actualDb = if (useSuiteLevelDatabase) testDatabaseName else db
    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$actualDb`")
      }

      spark.sql(
        s"""CREATE TABLE `$actualDb`.`$tbl` (
           |  ${schema.fields.map(_.toDDL).mkString(",\n  ")}
           |) USING ClickHouse
           |${if (partKeys.isEmpty) "" else partKeys.mkString("PARTITIONED BY(", ", ", ")")}
           |TBLPROPERTIES (
           |  ${if (sortKeys.isEmpty) "" else sortKeys.mkString("order_by = '", ", ", "',")}
           |  engine = '$engine'
           |)
           |""".stripMargin
      )

      if (isCloud) Thread.sleep(1000)

      f(actualDb, tbl)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(actualDb, tbl)
      } else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$actualDb`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$actualDb`")
      }
  }

  def withKVTable(
    db: String,
    tbl: String,
    keyColDef: String = "Int32",
    valueColDef: String
  )(f: (String, String) => Unit): Unit = {
    val actualDb = if (useSuiteLevelDatabase) testDatabaseName else db
    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$actualDb`")
      }
      runClickHouseSQL(
        s"""CREATE TABLE `$actualDb`.`$tbl` (
           |  key   $keyColDef,
           |  value $valueColDef
           |) ENGINE = MergeTree()
           |ORDER BY key
           |""".stripMargin
      )

      if (isCloud) Thread.sleep(1000)

      f(actualDb, tbl)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(actualDb, tbl)
      } else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$actualDb`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$actualDb`")
      }
  }

  def withSimpleTable(
    db: String,
    tbl: String,
    writeData: Boolean = false
  )(f: (String, String) => Unit): Unit = {
    val actualDb = if (useSuiteLevelDatabase) testDatabaseName else db
    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$actualDb`")
      }

      // SPARK-33779: Spark 3.3 only support IdentityTransform
      spark.sql(
        s"""CREATE TABLE `$actualDb`.`$tbl` (
           |  id          BIGINT    NOT NULL COMMENT 'sort key',
           |  value       STRING,
           |  create_time TIMESTAMP NOT NULL,
           |  m           INT       NOT NULL COMMENT 'part key'
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = 'id'
           |)
           |""".stripMargin
      )
      if (isCloud) Thread.sleep(1000)

      if (writeData) {
        val tblSchema = spark.table(s"$actualDb.$tbl").schema
        val dataDF = spark.createDataFrame(Seq(
          (1L, "1", timestamp("2021-01-01T10:10:10Z")),
          (2L, "2", timestamp("2022-02-02T10:10:10Z"))
        )).toDF("id", "value", "create_time")
          .withColumn("m", month($"create_time"))
          .select($"id", $"value", $"create_time", $"m")

        spark.createDataFrame(dataDF.rdd, tblSchema)
          .writeTo(s"$actualDb.$tbl")
          .append
      }

      f(actualDb, tbl)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(actualDb, tbl)
      } else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$actualDb`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$actualDb`")
      }
  }
}
