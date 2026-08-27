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
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import java.util.UUID
import scala.util.{Failure, Success, Try}

trait SparkClickHouseSingleTest extends SparkTest with ClickHouseProvider with BeforeAndAfterAll {

  import testImplicits._

  protected lazy val testDatabaseName: String = {
    val timestamp = System.currentTimeMillis()
    val uuidPrefix = UUID.randomUUID().toString.split("-").head
    s"test_db_${timestamp}_${uuidPrefix}"
  }

  protected val READ_SETTINGS_KEY = "spark.clickhouse.read.settings"

  protected def useSuiteLevelDatabase: Boolean = isCloud

  /**
   * Cloud's multi-replica compute can serve a read from a replica whose part or mutation cache has
   * not yet caught up with a recent write or DELETE. Retries `body` until reads converge, and runs
   * it once unchanged when not on Cloud.
   */
  protected def eventuallyOnCloud(body: => Unit): Unit =
    if (isCloud) eventually(timeout(15.seconds), interval(500.millis))(body) else body

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
    if (isCloud) Thread.sleep(2000)
  }

  override protected def dropTableWithRetry(db: String, tbl: String, maxRetries: Int = 5): Unit = {
    super.dropTableWithRetry(db, tbl, maxRetries)
    if (isCloud) Thread.sleep(500)
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
    .setAll(cloudReadSettings)

  /**
   * On Cloud a read may be routed to a replica that has not yet caught up with a just-committed
   * insert, silently returning an empty table. Only replicated engines honour the setting.
   */
  protected def cloudReadSettingValues: Seq[String] =
    if (isCloud) Seq("select_sequential_consistency=1") else Nil

  private def cloudReadSettings: Iterable[(String, String)] =
    if (cloudReadSettingValues.isEmpty) Nil
    else Seq(READ_SETTINGS_KEY -> cloudReadSettingValues.mkString(", "))

  /**
   * Read settings for a `withSQLConf(READ_SETTINGS_KEY -> ...)` override. The conf is single-valued,
   * so overriding it outright would drop [[cloudReadSettingValues]] and let the read land on a
   * replica that has not caught up.
   */
  protected def readSettingsWith(extra: String*): String =
    (cloudReadSettingValues ++ extra).mkString(", ")

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
    partKeys: Seq[String] = Seq.empty,
    extraProperties: Map[String, String] = Map.empty
  )(f: (String, String) => Unit): Unit = {
    val actualDb = if (useSuiteLevelDatabase) testDatabaseName else db
    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$actualDb`")
      }

      val tblProperties: String = {
        val props =
          sortKeys.headOption.map(_ => s"order_by = '${sortKeys.mkString(", ")}'").toSeq ++
            Seq(s"engine = '$engine'") ++
            extraProperties.toSeq.map { case (k, v) => s"'$k' = '$v'" }
        props.mkString(",\n  ")
      }

      spark.sql(
        s"""CREATE TABLE `$actualDb`.`$tbl` (
           |  ${schema.fields.map(_.toDDL).mkString(",\n  ")}
           |) USING ClickHouse
           |${if (partKeys.isEmpty) "" else partKeys.mkString("PARTITIONED BY(", ", ", ")")}
           |TBLPROPERTIES (
           |  $tblProperties
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
