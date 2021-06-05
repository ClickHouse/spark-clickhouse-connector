package clickhouse

import clickhouse.base.{BaseSparkSuite, ClickHouseSingleSuiteMixIn}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import xenon.clickhouse.Logging

class ClickHouseSingleSuite extends BaseSparkSuite with ClickHouseSingleSuiteMixIn with Logging {

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[1]",
    "spark.ui.enabled" -> "false", // enable when debug
    "spark.app.name" -> "spark-clickhouse-single-ut",
    "spark.sql.shuffle.partitions" -> "1",
    "spark.sql.defaultCatalog" -> "clickhouse",
    "spark.sql.catalog.clickhouse" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host" -> clickhouseHost,
    "spark.sql.catalog.clickhouse.port" -> clickhouseGrpcPort.toString,
    "spark.sql.catalog.clickhouse.user" -> CLICKHOUSE_USER,
    "spark.sql.catalog.clickhouse.password" -> CLICKHOUSE_PASSWORD,
    "spark.sql.catalog.clickhouse.database" -> CLICKHOUSE_DB,
    "spark.clickhouse.write.batchSize" -> "2",
    "spark.clickhouse.write.maxRetry" -> "2",
    "spark.clickhouse.write.retryInterval" -> "1",
    "spark.clickhouse.write.retryableErrorCodes" -> "241"
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

    import spark.implicits._

    spark.sql(
      """
        | CREATE TABLE default.spark_tbl (
        |   create_time TIMESTAMP NOT NULL,
        |   id          BIGINT    NOT NULL,
        |   value       STRING
        | ) USING ClickHouse
        | PARTITIONED BY (months(create_time))
        | TBLPROPERTIES (
        |   engine = 'MergeTree()',
        |   order_by = '(id)',
        |   settings.index_granularity = 8192
        | )
        |""".stripMargin
    )

    spark.sql("DESC default.spark_tbl").show(false)
    // +--------------+-------------------+-------+
    // |col_name      |data_type          |comment|
    // +--------------+-------------------+-------+
    // |create_time   |timestamp          |       |
    // |id            |bigint             |       |
    // |value         |string             |       |
    // |              |                   |       |
    // |# Partitioning|                   |       |
    // |Part 0        |months(create_time)|       |
    // +--------------+-------------------+-------+

    val tblSchema = spark.table("default.spark_tbl").schema

    assert(tblSchema == StructType(
      StructField("create_time", DataTypes.TimestampType, false) ::
        StructField("id", DataTypes.LongType, false) ::
        StructField("value", DataTypes.StringType, true) :: Nil
    ))

    val dataDF = spark.createDataFrame(Seq(
      ("2021-01-01 10:10:10", 1L, "1"),
      ("2021-02-02 10:10:10", 2L, "2")
    )).toDF("create_time", "id", "value")
      .withColumn("create_time", to_timestamp($"create_time"))

    val dataDFWithExactlySchema = spark.createDataFrame(dataDF.rdd, tblSchema)

    dataDFWithExactlySchema
      .writeTo("clickhouse.default.spark_tbl")
      .append

    spark.table("default.spark_tbl").show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2021-01-01 10:10:10|1  |1    |
    // |2021-02-02 10:10:10|2  |2    |
    // +-------------------+---+-----+

    spark.table("default.spark_tbl")
      .filter($"id" > 1)
      .show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2021-02-02 10:10:10|2  |2    |
    // +-------------------+---+-----+

    spark.table("default.spark_tbl")
      .filter($"id" > 1)
      .count
    // 1

    // infiniteLoop()
  }
}
