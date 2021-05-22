package xenon.clickhouse

import xenon.clickhouse.Utils.PREFIX

class ClickHouseClusterSuite extends ClickHouseClusterSuiteMixIn with Logging {

  Utils.setTesting()

  test("clickhouse cluster up") {
    assert(Utils.isTesting)

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
