package xenon.clickhouse.base

trait SparkClickHouseSingleSuiteMixin { self: BaseSparkSuite with ClickHouseSingleSuiteMixIn =>

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[2]",
    "spark.ui.enabled" -> "false", // enable when debug
    "spark.app.name" -> "spark-clickhouse-single-ut",
    "spark.sql.shuffle.partitions" -> "2",
    "spark.sql.defaultCatalog" -> "clickhouse",
    "spark.sql.catalog.clickhouse" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host" -> clickhouseHost,
    "spark.sql.catalog.clickhouse.grpc_port" -> clickhouseGrpcPort.toString,
    "spark.sql.catalog.clickhouse.user" -> CLICKHOUSE_USER,
    "spark.sql.catalog.clickhouse.password" -> CLICKHOUSE_PASSWORD,
    "spark.sql.catalog.clickhouse.database" -> CLICKHOUSE_DB,
    // extended configurations
    "spark.clickhouse.write.batchSize" -> "2",
    "spark.clickhouse.write.maxRetry" -> "2",
    "spark.clickhouse.write.retryInterval" -> "1",
    "spark.clickhouse.write.retryableErrorCodes" -> "241",
    "spark.clickhouse.write.write.repartitionNum" -> "0"
  )

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouseHost,
    "grpc_port" -> clickhouseGrpcPort.toString,
    "user" -> CLICKHOUSE_USER,
    "password" -> CLICKHOUSE_PASSWORD,
    "database" -> CLICKHOUSE_DB
  )
}
