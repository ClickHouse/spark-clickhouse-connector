package xenon.clickhouse

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

abstract class BaseClickHouseSingleSuite extends ClickHouseSingleSuiteMixIn {

  Utils.setTesting()

  /**
   * The spark session, which is the entrance point of DataFrame, DataSet and Spark SQL.
   */
  @transient implicit lazy val spark: SparkSession = {
    val builder = SparkSession.builder().appName(this.getClass.getName)

    builder
      .master("local[1]")
      .appName("SparkUnitTesting")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.memory", "500M")
      .config("spark.executor.memory", "500M")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      .config("spark.sql.defaultCatalog", "clickhouse")
      .config("spark.sql.catalog.clickhouse", "xenon.clickhouse.ClickHouseCatalog")
      .config("spark.sql.catalog.clickhouse.host", clickhouseHost)
      .config("spark.sql.catalog.clickhouse.port", clickhouseGrpcPort)
      .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
      .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD)
      .config("spark.sql.catalog.clickhouse.database", CLICKHOUSE_DB)
    // for test, hive support is not enabled. Use in-memory catalog implementation

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

  val cmdRunner: String = classOf[ClickHouseCommandRunner].getName

  lazy val cmdRunnerOptions = Map(
    "host" -> clickhouseHost,
    "port" -> clickhouseGrpcPort.toString,
    "user" -> CLICKHOUSE_USER,
    "password" -> CLICKHOUSE_PASSWORD,
    "database" -> CLICKHOUSE_DB
  )

  def runClickHouseSQL(sql: String, options: Map[String, String] = cmdRunnerOptions): Array[Row] =
    spark.executeCommand(cmdRunner, sql, options).collect

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
}
