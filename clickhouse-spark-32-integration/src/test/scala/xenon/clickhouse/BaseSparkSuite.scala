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

package xenon.clickhouse

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

abstract class BaseSparkSuite extends AnyFunSuite with BeforeAndAfterAll with Eventually {

  Utils.setTesting()

  test("is testing") {
    assert(Utils.isTesting)
  }

  def sparkOptions: Map[String, String]

  def cmdRunnerOptions: Map[String, String]

  /**
   * The spark session, which is the entrance point of DataFrame, DataSet and Spark SQL.
   */
  @transient implicit lazy val spark: SparkSession = {
    val builder = SparkSession.builder()

    builder
      .config("spark.master", "local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.app.name", "spark-ut")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.memory", "500M")
      .config("spark.executor.memory", "500M")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.shuffle.partitions", "2")

    sparkOptions.foreach { case (k, v) => builder.config(k, v) }

    builder.getOrCreate
  }

  /**
   * The spark context
   */
  @transient lazy val sc: SparkContext = spark.sparkContext

  /**
   * The spark file system
   */
  @transient lazy val fs: FileSystem = {
    val hadoopConf = sc.hadoopConfiguration
    FileSystem.get(hadoopConf)
  }

  def runClickHouseSQL(sql: String, options: Map[String, String] = cmdRunnerOptions): DataFrame =
    spark.executeCommand(classOf[ClickHouseCommandRunner].getName, sql, options)

  def withClickHouseSingleIdTable(
    database: String,
    table: String,
    cleanup: Boolean = true
  )(block: (String, String) => Unit): Unit =
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS `$database`")
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS `$database`.`$table` (
           |  id Long NOT NULL
           |) USING ClickHouse
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = '(id)',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )
      block(database, table)
    } finally if (cleanup) {
      spark.sql(s"DROP TABLE IF EXISTS `$database`.`$table`")
      spark.sql(s"DROP DATABASE IF EXISTS `$database`")
    }

  // for debugging webui
  protected def infiniteLoop(): Unit = while (true) {
    Thread.sleep(1000)
    spark.catalog.listTables()
  }

  override def afterAll(): Unit = {
    spark.stop
    super.afterAll()
  }
}
