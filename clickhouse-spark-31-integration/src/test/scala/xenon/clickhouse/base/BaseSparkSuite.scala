package xenon.clickhouse.base

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.clickhouse.SparkUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.{ClickHouseCommandRunner, Utils}

abstract class BaseSparkSuite extends AnyFunSuite with BeforeAndAfterAll {

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
      .config("spark.master", "local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.app.name", "spark-ut")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.memory", "500M")
      .config("spark.executor.memory", "500M")
      .config("spark.sql.shuffle.partitions", "1")

    sparkOptions.foreach { case (k, v) => builder.config(k, v) }

    builder.getOrCreate()
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

  def runClickHouseSQL(sql: String, options: Map[String, String] = cmdRunnerOptions): Array[Row] =
    spark.executeCommand(classOf[ClickHouseCommandRunner].getName, sql, options).collect

  def withClickHouseSingleIdTable(
    database: String,
    table: String,
    cleanup: Boolean = true
  )(block: (String, String) => Unit): Unit =
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS `$database`")
      spark.sql(
        s"""
           | CREATE TABLE IF NOT EXISTS `$database`.`$table` (
           |   id Long NOT NULL
           | ) USING ClickHouse
           | TBLPROPERTIES (
           | engine = 'MergeTree()',
           | order_by = '(id)',
           | settings.index_granularity = 8192
           | )
           | """.stripMargin
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

  def spark_32_only(testFun: => Any): Unit = {
    assume(
      SparkUtils.MAJOR_MINOR_VERSION match {
        case (3, 2) => true
        case _ => false
      },
      "The test only for Spark 3.2"
    )
    testFun
  }
}
