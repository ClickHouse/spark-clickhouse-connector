package xenon.clickhouse.base

trait SparkClickHouseSingleSuiteMixin { self: BaseSparkSuite with ClickHouseSingleSuiteMixIn =>

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[1]",
    "spark.ui.enabled" -> "false", // enable when debug
    "spark.app.name" -> "spark-clickhouse-single-ut",
    "spark.sql.shuffle.partitions" -> "1",
    "spark.sql.defaultCatalog" -> "clickhouse",
    "spark.sql.catalog.clickhouse" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host" -> clickhouseHost,
    "spark.sql.catalog.clickhouse.grpc_port" -> clickhouseGrpcPort.toString,
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
    "grpc_port" -> clickhouseGrpcPort.toString,
    "user" -> CLICKHOUSE_USER,
    "password" -> CLICKHOUSE_PASSWORD,
    "database" -> CLICKHOUSE_DB
  )
}
