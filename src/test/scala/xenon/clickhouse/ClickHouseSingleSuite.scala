package xenon.clickhouse

import xenon.clickhouse.base.{BaseSparkSuite, ClickHouseSingleSuiteMixIn}

class ClickHouseSingleSuite extends BaseSparkSuite with ClickHouseSingleSuiteMixIn with Logging {

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[1]",
    "spark.app.name" -> "spark-clickhouse-single-ut",
    "spark.sql.shuffle.partitions" -> "1",
    "spark.sql.defaultCatalog" -> "clickhouse",
    "spark.sql.catalog.clickhouse" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host" -> clickhouseHost,
    "spark.sql.catalog.clickhouse.port" -> clickhouseGrpcPort.toString,
    "spark.sql.catalog.clickhouse.user" -> CLICKHOUSE_USER,
    "spark.sql.catalog.clickhouse.password" -> CLICKHOUSE_PASSWORD,
    "spark.sql.catalog.clickhouse.database" -> CLICKHOUSE_DB
  )

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouseHost,
    "port" -> clickhouseGrpcPort.toString,
    "user" -> CLICKHOUSE_USER,
    "password" -> CLICKHOUSE_PASSWORD,
    "database" -> CLICKHOUSE_DB
  )

  test("clickhouse command runner") {
    runClickHouseSQL("CREATE TABLE default.abc(a UInt8) ENGINE=Log()")
    spark.sql(""" DESC default.abc """).show(false)
    runClickHouseSQL("DROP TABLE default.abc")
  }

  test("clickhouse catalog") {
    spark.sql("CREATE DATABASE test_1").collect
    spark.sql("CREATE DATABASE test_2").collect
    spark.sql("SHOW DATABASES").show(false)
    spark.sql("USE system").collect
    spark.sql("SHOW tables").show(false)
  }

  // TRUNCATE TABLE is not supported for v2 tables
  ignore("clickhouse truncate table") {
    withClickHouseSingleIdTable("db_trunc", "tbl_trunc") { (db, tbl) =>
      spark.range(10).toDF("id").writeTo(s"$db.$tbl")
      assert(spark.table(s"$db.$tbl").count == 10)
      spark.sql(s"TRUNCATE TABLE $db.$tbl")
      assert(spark.table(s"$db.$tbl").count == 0)
    }
  }

  test("clickhouse write then read") {
    spark.sql(
      """
        | CREATE TABLE default.spark_tbl (
        |   a INT NOT NULL,
        |   b LONG NOT NULL,
        |   c STRING
        | ) USING ClickHouse
        | PARTITIONED BY (toDate(a))
        | TBLPROPERTIES (
        | engine = 'MergeTree()',
        | order_by = '(b)',
        | settings.index_granularity = 8192
        | )
        |""".stripMargin)

    spark.createDataFrame(Seq((1, 1L, "1"), (2, 2L, "2")))
      .toDF("a", "b", "c")
      .writeTo("clickhouse.default.spark_tbl")
      .append

    spark.table("default.spark_tbl").show(false)
  }
}
