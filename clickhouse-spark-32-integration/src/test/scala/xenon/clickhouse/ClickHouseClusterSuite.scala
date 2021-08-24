package xenon.clickhouse

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import xenon.clickhouse.base._

class ClickHouseClusterSuite extends BaseSparkSuite
    with ClickHouseClusterSuiteMixIn
    with SparkClickHouseClusterSuiteMixin
    with Logging {

  test("clickhouse write cluster") {

    import spark.implicits._

    spark.sql(
      """
        | CREATE TABLE default.t_dist_local (
        |   create_time TIMESTAMP NOT NULL,
        |   y           INT       NOT NULL,
        |   m           INT       NOT NULL,
        |   id          BIGINT    NOT NULL,
        |   value       STRING
        | ) USING ClickHouse
        | PARTITIONED BY (m)
        | TBLPROPERTIES (
        |   cluster = 'single_replica',
        |   engine = 'MergeTree()',
        |   order_by = '(id)',
        |   settings.index_granularity = 8192
        | )
        |""".stripMargin
    )

    runClickHouseSQL(
      """
        | CREATE TABLE default.t_dist ON CLUSTER default
        | AS default.t_dist_local
        | ENGINE = Distributed(single_replica, 'default', 't_dist_local', y)
        |""".stripMargin
    )

    spark.sql("DESC default.t_dist").show(false)
    // +--------------+---------+-------+
    // |col_name      |data_type|comment|
    // +--------------+---------+-------+
    // |create_time   |timestamp|       |
    // |y             |y        |       |
    // |m             |m        |       |
    // |id            |bigint   |       |
    // |value         |string   |       |
    // |              |         |       |
    // |# Partitioning|         |       |
    // |Part 0        |y        |       |
    // |Part 1        |m        |       |
    // +--------------+---------+-------+

    val tblSchema = spark.table("default.t_dist").schema

    assert(tblSchema == StructType(
      StructField("create_time", DataTypes.TimestampType, false) ::
        StructField("y", DataTypes.IntegerType, false) ::
        StructField("m", DataTypes.IntegerType, false) ::
        StructField("id", DataTypes.LongType, false) ::
        StructField("value", DataTypes.StringType, true) :: Nil
    ))

    val dataDF = spark.createDataFrame(Seq(
      ("2021-01-01 10:10:10", 1L, "1"),
      ("2022-02-02 10:10:10", 2L, "2"),
      ("2023-03-03 10:10:10", 3L, "3"),
      ("2024-04-04 10:10:10", 4L, "4")
    )).toDF("create_time", "id", "value")
      .withColumn("create_time", to_timestamp($"create_time"))
      .withColumn("y", year($"create_time"))
      .withColumn("m", month($"create_time"))
      .select($"create_time", $"y", $"m", $"id", $"value")

    val dataDFWithExactlySchema = spark.createDataFrame(dataDF.rdd, tblSchema)

    dataDFWithExactlySchema
      .writeTo("`clickhouse-s1r1`.default.t_dist")
      .append

    log.info("==== [Distributed] default.t_dist_local ====")
    spark.table("default.t_dist").show(false)
    // +-------------------+----+---+---+-----+
    // |create_time        |y   |m  |id |value|
    // +-------------------+----+---+---+-----+
    // |2024-04-04 10:10:10|2024|4  |4  |4    |
    // |2021-01-01 10:10:10|2021|1  |1  |1    |
    // |2022-02-02 10:10:10|2022|2  |2  |2    |
    // |2023-03-03 10:10:10|2023|3  |3  |3    |
    // +-------------------+----+---+---+-----+

    log.info("==== [Local] clickhouse-s1r1.default.t_dist_local ====")
    spark.table("`clickhouse-s1r1`.default.t_dist_local").show(false)
    // +-------------------+----+---+---+-----+
    // |create_time        |y   |m  |id |value|
    // +-------------------+----+---+---+-----+
    // |2024-04-04 10:10:10|2024|4  |4  |4    |
    // +-------------------+----+---+---+-----+

    log.info("==== [Local] clickhouse-s1r2.default.t_dist_local ====")
    spark.table("`clickhouse-s1r2`.default.t_dist_local").show(false)
    // +-------------------+----+---+---+-----+
    // |create_time        |y   |m  |id |value|
    // +-------------------+----+---+---+-----+
    // |2021-01-01 10:10:10|2021|1  |1  |1    |
    // +-------------------+----+---+---+-----+

    log.info("==== [Local] clickhouse-s2r1.default.t_dist_local ====")
    spark.table("`clickhouse-s2r1`.default.t_dist_local").show(false)
    // +-------------------+----+---+---+-----+
    // |create_time        |y   |m  |id |value|
    // +-------------------+----+---+---+-----+
    // |2022-02-02 10:10:10|2022|2  |2  |2    |
    // +-------------------+----+---+---+-----+

    log.info("==== [Local] clickhouse-s2r2.default.t_dist_local ====")
    spark.table("`clickhouse-s2r2`.default.t_dist_local").show(false)
    // +-------------------+----+---+---+-----+
    // |create_time        |y   |m  |id |value|
    // +-------------------+----+---+---+-----+
    // |2023-03-03 10:10:10|2023|3  |3  |3    |
    // +-------------------+----+---+---+-----+

    // infiniteLoop()
  }
}
