package xenon.clickhouse

import xenon.clickhouse.base.{BaseSparkSuite, ClickHouseClusterSuiteMixIn, SparkClickHouseClusterSuiteMixin}

class ClickHouseUDFSuite extends BaseSparkSuite
  with ClickHouseClusterSuiteMixIn
  with SparkClickHouseClusterSuiteMixin
    with Logging {

  override def beforeAll: Unit =
    ClickHouseUDF.register

  test("UDF ck_xx_hash64") {

    spark.sql(
      """
        | SELECT
        |   ck_xx_hash64('spark-clickhouse-connector')          AS hash_value,
        |   ck_xx_hash64_shard(4, 'spark-clickhouse-connector') AS shard_num
        |""".stripMargin)
      .show(false)

    runClickHouseSQL(
      """
        | SELECT
        |   xxHash64('spark-clickhouse-connector')     AS hash_value,
        |   xxHash64('spark-clickhouse-connector') % 4 AS shard_num
        |""".stripMargin)
      .foreach(println)
  }
}
