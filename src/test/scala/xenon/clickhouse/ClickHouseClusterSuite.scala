package xenon.clickhouse

import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import xenon.clickhouse.base.{BaseSparkSuite, ClickHouseClusterSuiteMixIn}

class ClickHouseClusterSuite extends BaseSparkSuite with ClickHouseClusterSuiteMixIn with Logging {

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[4]",
    "spark.ui.enabled" -> "false", // enable when debug
    "spark.app.name" -> "spark-clickhouse-cluster-ut",
    "spark.sql.shuffle.partitions" -> "4",
    "spark.sql.defaultCatalog" -> "clickhouse-s1r1",
    "spark.sql.catalog.clickhouse-s1r1" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s1r1.host" -> clickhouse_s1r1_host,
    "spark.sql.catalog.clickhouse-s1r1.port" -> clickhouse_s1r1_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s1r1.user" -> "default",
    "spark.sql.catalog.clickhouse-s1r1.password" -> "",
    "spark.sql.catalog.clickhouse-s1r1.database" -> "default",
    "spark.sql.catalog.clickhouse-s1r2" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s1r2.host" -> clickhouse_s1r2_host,
    "spark.sql.catalog.clickhouse-s1r2.port" -> clickhouse_s1r2_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s1r2.user" -> "default",
    "spark.sql.catalog.clickhouse-s1r2.password" -> "",
    "spark.sql.catalog.clickhouse-s1r2.database" -> "default",
    "spark.sql.catalog.clickhouse-s2r1" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s2r1.host" -> clickhouse_s2r1_host,
    "spark.sql.catalog.clickhouse-s2r1.port" -> clickhouse_s2r1_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s2r1.user" -> "default",
    "spark.sql.catalog.clickhouse-s2r1.password" -> "",
    "spark.sql.catalog.clickhouse-s2r1.database" -> "default",
    "spark.sql.catalog.clickhouse-s2r2" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s2r2.host" -> clickhouse_s2r2_host,
    "spark.sql.catalog.clickhouse-s2r2.port" -> clickhouse_s2r2_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s2r2.user" -> "default",
    "spark.sql.catalog.clickhouse-s2r2.password" -> "",
    "spark.sql.catalog.clickhouse-s2r2.database" -> "default",
    "spark.clickhouse.write.batchSize" -> "2",
    "spark.clickhouse.write.maxRetry" -> "2",
    "spark.clickhouse.write.retryInterval" -> "1",
    "spark.clickhouse.write.retryableErrorCodes" -> "241",
    "spark.clickhouse.write.distributed.useClusterNodes" -> "true",
    "spark.clickhouse.read.distributed.useClusterNodes" -> "true",
    "spark.clickhouse.write.distributed.convertLocal" -> "false",
    "spark.clickhouse.read.distributed.convertLocal" -> "false",
    "spark.clickhouse.truncate.distributed.convertLocal" -> "true"
  )

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouse_s1r1_host,
    "port" -> clickhouse_s1r1_grpc_port.toString,
    "user" -> "default",
    "password" -> "",
    "database" -> "default"
  )

  test("clickhouse write cluster") {
    import spark.implicits._

    spark.sql(
      """
        | CREATE TABLE default.t_dist_local (
        |   create_time TIMESTAMP NOT NULL,
        |   id          BIGINT    NOT NULL,
        |   value       STRING
        | ) USING ClickHouse
        | PARTITIONED BY (months(create_time))
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
        | ENGINE = Distributed(single_replica, 'default', 't_dist_local', YEAR(create_time))
        |""".stripMargin
    )

    spark.sql("DESC default.t_dist").show(false)
    // +--------------+-------------------+-------+
    // |col_name      |data_type          |comment|
    // +--------------+-------------------+-------+
    // |create_time   |timestamp          |       |
    // |id            |bigint             |       |
    // |value         |string             |       |
    // |              |                   |       |
    // |# Partitioning|                   |       |
    // |Part 0        |years(create_time) |       |
    // |Part 1        |months(create_time)|       |
    // +--------------+-------------------+-------+

    val tblSchema = spark.table("default.t_dist").schema

    assert(tblSchema == StructType(
      StructField("create_time", DataTypes.TimestampType, false) ::
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

    val dataDFWithExactlySchema = spark.createDataFrame(dataDF.rdd, tblSchema)

    dataDFWithExactlySchema
      .writeTo("`clickhouse-s1r1`.default.t_dist")
      .append

    log.info("==== [Distributed] default.t_dist_local ====")
    spark.table("default.t_dist").show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2024-04-04 10:10:10|4  |4    |
    // |2022-02-02 10:10:10|2  |2    |
    // |2021-01-01 10:10:10|1  |1    |
    // |2023-03-03 10:10:10|3  |3    |
    // +-------------------+---+-----+

    log.info("==== [Local] clickhouse-s1r1.default.t_dist_local ====")
    spark.table("`clickhouse-s1r1`.default.t_dist_local").show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2024-04-04 10:10:10|4  |4    |
    // +-------------------+---+-----+

    log.info("==== [Local] clickhouse-s1r2.default.t_dist_local ====")
    spark.table("`clickhouse-s1r2`.default.t_dist_local").show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2021-01-01 10:10:10|1  |1    |
    // +-------------------+---+-----+


    log.info("==== [Local] clickhouse-s2r1.default.t_dist_local ====")
    spark.table("`clickhouse-s2r1`.default.t_dist_local").show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2022-02-02 10:10:10|2  |2    |
    // +-------------------+---+-----+

    log.info("==== [Local] clickhouse-s2r2.default.t_dist_local ====")
    spark.table("`clickhouse-s2r2`.default.t_dist_local").show(false)
    // +-------------------+---+-----+
    // |create_time        |id |value|
    // +-------------------+---+-----+
    // |2023-03-03 10:10:10|3  |3    |
    // +-------------------+---+-----+

    // infiniteLoop()
  }
}
