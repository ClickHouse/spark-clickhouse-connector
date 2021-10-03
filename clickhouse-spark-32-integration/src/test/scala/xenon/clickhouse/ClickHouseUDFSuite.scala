package xenon.clickhouse

import java.lang.{Long => JLong}

import xenon.clickhouse.JsonProtocol.om
import xenon.clickhouse.base.{BaseSparkSuite, ClickHouseClusterSuiteMixIn, SparkClickHouseClusterSuiteMixin}

class ClickHouseUDFSuite extends BaseSparkSuite
    with ClickHouseClusterSuiteMixIn
    with SparkClickHouseClusterSuiteMixin
    with Logging {

  override def beforeAll: Unit =
    ClickHouseUDF.register

  test("UDF ck_xx_hash64") {
    val stringVal = "spark-clickhouse-connector"

    val sparkResult = spark.sql(
      s"""
         |SELECT
         |  ck_xx_hash64('$stringVal')          AS hash_value,
         |  ck_xx_hash64_shard(4, '$stringVal') AS shard_num
         |""".stripMargin
    ).collect
    assert(sparkResult.length == 1)
    val sparkHashVal = sparkResult.head.getAs[Long]("hash_value")
    val sparkShardNum = sparkResult.head.getAs[Long]("shard_num")

    val clickhouseResultJsonStr = runClickHouseSQL(
      s"""
         |SELECT
         |  xxHash64('$stringVal')     AS hash_value,
         |  xxHash64('$stringVal') % 4 AS shard_num
         |""".stripMargin
    ).head.getString(0)
    val clickhouseResultJson = om.readTree(clickhouseResultJsonStr)
    val clickhouseHashVal = JLong.parseUnsignedLong(clickhouseResultJson.get("hash_value").asText)
    val clickhouseShardNum = JLong.parseUnsignedLong(clickhouseResultJson.get("shard_num").asText)

    assert(sparkHashVal == clickhouseHashVal)
    assert(sparkShardNum == clickhouseShardNum)
  }
}
