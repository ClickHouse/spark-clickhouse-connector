package xenon.clickhouse

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import xenon.clickhouse.base.{BaseSparkSuite, ClickHouseSingleSuiteMixIn, SparkClickHouseSingleSuiteMixin}

class ClickHouseSingleSuite extends BaseSparkSuite
    with ClickHouseSingleSuiteMixIn
    with SparkClickHouseSingleSuiteMixin
    with Logging {

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

  test("clickhouse truncate table") {
    spark_32_only {
      withClickHouseSingleIdTable("db_trunc", "tbl_trunc") { (db, tbl) =>
        spark.range(10).toDF("id").writeTo(s"$db.$tbl").append
        assert(spark.table(s"$db.$tbl").count == 10)
        spark.sql(s"TRUNCATE TABLE $db.$tbl")
        spark.sql(s"REFRESH TABLE $db.$tbl")
        assert(spark.table(s"$db.$tbl").count == 0)
      }
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
