package xenon.clickhouse

import xenon.clickhouse.Utils.PREFIX
import xenon.clickhouse.base.{BaseSparkSuite, ClickHouseClusterSuiteMixIn}

class ClickHouseClusterSuite extends BaseSparkSuite with ClickHouseClusterSuiteMixIn with Logging {

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[4]",
    "spark.app.name" -> "spark-clickhouse-cluster-ut",
    "spark.sql.shuffle.partitions" -> "4",
    "spark.sql.defaultCatalog" -> "clickhouse",
    "spark.sql.catalog.clickhouse" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host" -> clickhouse_s1r1_host,
    "spark.sql.catalog.clickhouse.port" -> clickhouse_s1r1_grpc_port.toString,
    "spark.sql.catalog.clickhouse.user" -> "default",
    "spark.sql.catalog.clickhouse.password" -> "",
    "spark.sql.catalog.clickhouse.database" -> "default",
    "spark.sql.catalog.clickhouse.prefer-local-table" -> "true"
  )

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouse_s1r1_host,
    "port" -> clickhouse_s1r1_grpc_port.toString,
    "user" -> "default",
    "password" -> "",
    "database" -> "default"
  )

  test("clickhouse cluster up") {
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r1").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r1_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r1_PORT_$CLICKHOUSE_GRPC_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r1_PORT_$CLICKHOUSE_TCP_PORT").isDefined)

    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r2").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r2_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r2_PORT_$CLICKHOUSE_GRPC_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s1r2_PORT_$CLICKHOUSE_TCP_PORT").isDefined)

    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r1").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r1_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r1_PORT_$CLICKHOUSE_GRPC_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r1_PORT_$CLICKHOUSE_TCP_PORT").isDefined)

    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r2").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r2_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r2_PORT_$CLICKHOUSE_GRPC_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse_s2r2_PORT_$CLICKHOUSE_TCP_PORT").isDefined)
  }
}
