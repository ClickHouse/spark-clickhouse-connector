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

package org.apache.spark.sql.clickhouse

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}
import xenon.clickhouse.ClickHouseCommandRunner

trait SparkTest extends QueryTest with SharedSparkSession {

  def cmdRunnerOptions: Map[String, String]

  override protected def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[2]")
    .setAppName("spark-ut")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.host", "localhost")
    .set("spark.driver.memory", "500M")
    .set("spark.sql.catalogImplementation", "in-memory")
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.shuffle.partitions", "2")

  def runClickHouseSQL(sql: String, options: Map[String, String] = cmdRunnerOptions): DataFrame =
    spark.executeCommand(classOf[ClickHouseCommandRunner].getName, sql, options)

  def autoCleanupTable(
    database: String,
    table: String,
    cleanup: Boolean = true
  )(block: (String, String) => Unit): Unit =
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS `$database`")
      block(database, table)
    } finally if (cleanup) {
        spark.sql(s"DROP TABLE IF EXISTS `$database`.`$table`")
        spark.sql(s"DROP DATABASE IF EXISTS `$database` CASCADE")
      }

  def withClickHouseSingleIdTable(
    database: String,
    table: String,
    cleanup: Boolean = true
  )(block: (String, String) => Unit): Unit = autoCleanupTable(database, table, cleanup) { (database, table) =>
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS `$database`.`$table` (
         |  id Long NOT NULL
         |) USING ClickHouse
         |TBLPROPERTIES (
         |  engine = 'MergeTree()',
         |  order_by = 'id',
         |  settings.index_granularity = 8192
         |)
         |""".stripMargin
    )
    block(database, table)
  }

  // for debugging webui
  protected def infiniteLoop(): Unit = while (true) {
    Thread.sleep(1000)
    spark.catalog.listTables()
  }
}
