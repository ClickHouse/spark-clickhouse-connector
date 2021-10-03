package xenon.clickhouse.cluster

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.QueryTest._
import org.apache.spark.sql.Row
import xenon.clickhouse.base._
import xenon.clickhouse.Logging

abstract class BaseClusterWriteSuite extends BaseSparkSuite
    with ClickHouseClusterSuiteMixIn
    with SparkClickHouseClusterSuiteMixin
    with Logging {

  test("clickhouse write cluster") {

    import spark.implicits._

    spark.sql(
      """
        |CREATE TABLE default.t_dist_local (
        |  create_time TIMESTAMP NOT NULL,
        |  y           INT       NOT NULL,
        |  m           INT       NOT NULL,
        |  id          BIGINT    NOT NULL,
        |  value       STRING
        |) USING ClickHouse
        |PARTITIONED BY (m)
        |TBLPROPERTIES (
        |  cluster = 'single_replica',
        |  engine = 'MergeTree()',
        |  order_by = '(id)',
        |  settings.index_granularity = 8192
        |)
        |""".stripMargin
    )

    runClickHouseSQL(
      """
        |CREATE TABLE default.t_dist ON CLUSTER default
        |AS default.t_dist_local
        |ENGINE = Distributed(single_replica, 'default', 't_dist_local', y)
        |""".stripMargin
    )

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

    checkAnswer(
      spark.table("default.t_dist"),
      Seq(
        Row(Timestamp.valueOf("2021-01-01 10:10:10"), 2021, 1, 1L, "1"),
        Row(Timestamp.valueOf("2022-02-02 10:10:10"), 2022, 2, 2L, "2"),
        Row(Timestamp.valueOf("2023-03-03 10:10:10"), 2023, 3, 3L, "3"),
        Row(Timestamp.valueOf("2024-04-04 10:10:10"), 2024, 4, 4L, "4")
      )
    )

    checkAnswer(
      spark.table("`clickhouse-s1r1`.default.t_dist_local"),
      Row(Timestamp.valueOf("2024-04-04 10:10:10"), 2024, 4, 4L, "4") :: Nil
    )

    checkAnswer(
      spark.table("`clickhouse-s1r2`.default.t_dist_local"),
      Row(Timestamp.valueOf("2021-01-01 10:10:10"), 2021, 1, 1L, "1") :: Nil
    )
    checkAnswer(
      spark.table("`clickhouse-s2r1`.default.t_dist_local"),
      Row(Timestamp.valueOf("2022-02-02 10:10:10"), 2022, 2, 2L, "2") :: Nil
    )
    checkAnswer(
      spark.table("`clickhouse-s2r2`.default.t_dist_local"),
      Row(Timestamp.valueOf("2023-03-03 10:10:10"), 2023, 3, 3L, "3") :: Nil
    )

    // infiniteLoop()
  }
}
