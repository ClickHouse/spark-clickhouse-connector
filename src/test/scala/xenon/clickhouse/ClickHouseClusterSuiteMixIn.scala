package xenon.clickhouse

import java.io.File

import com.dimafeng.testcontainers._
import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.Utils._

trait ClickHouseClusterSuiteMixIn extends AnyFunSuite with ForAllTestContainer {

  protected val ZOOKEEPER_CLIENT_PORT = 2181
  protected val CLICKHOUSE_HTTP_PORT = 8123
  protected val CLICKHOUSE_GRPC_PORT = 9100
  protected val CLICKHOUSE_TCP_PORT  = 9000

  override val container: DockerComposeContainer =
    DockerComposeContainer
      .Def(
        composeFiles = new File(Utils.classpathResource("clickhouse-cluster/clickhouse-s2r2-compose.yml")),
        exposedServices = ExposedService("zookeeper", ZOOKEEPER_CLIENT_PORT) ::
          // s1r1
          ExposedService("clickhouse-s1r1", CLICKHOUSE_HTTP_PORT) ::
          ExposedService("clickhouse-s1r1", CLICKHOUSE_GRPC_PORT) ::
          ExposedService("clickhouse-s1r1", CLICKHOUSE_TCP_PORT) ::
          // s1r2
          ExposedService("clickhouse-s1r2", CLICKHOUSE_HTTP_PORT) ::
          ExposedService("clickhouse-s1r2", CLICKHOUSE_GRPC_PORT) ::
          ExposedService("clickhouse-s1r2", CLICKHOUSE_TCP_PORT) ::
          // s2r1
          ExposedService("clickhouse-s2r1", CLICKHOUSE_HTTP_PORT) ::
          ExposedService("clickhouse-s2r1", CLICKHOUSE_GRPC_PORT) ::
          ExposedService("clickhouse-s2r1", CLICKHOUSE_TCP_PORT) ::
          // s2r2
          ExposedService("clickhouse-s2r2", CLICKHOUSE_HTTP_PORT) ::
          ExposedService("clickhouse-s2r2", CLICKHOUSE_GRPC_PORT) ::
          ExposedService("clickhouse-s2r2", CLICKHOUSE_TCP_PORT) :: Nil,
      )
      .createContainer()

  // format: off
  // s1r1
  def clickhouse_s1r1_host:   String = container.getServiceHost("clickhouse-s1r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r1_http_port: Int = container.getServicePort("clickhouse-s1r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r1_grpc_port: Int = container.getServicePort("clickhouse-s1r1", CLICKHOUSE_GRPC_PORT)
  def clickhouse_s1r1_tcp_port:  Int = container.getServicePort("clickhouse-s1r1", CLICKHOUSE_TCP_PORT)
  // s1r2
  def clickhouse_s1r2_host:   String = container.getServiceHost("clickhouse-s1r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r2_http_port: Int = container.getServicePort("clickhouse-s1r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r2_grpc_port: Int = container.getServicePort("clickhouse-s1r2", CLICKHOUSE_GRPC_PORT)
  def clickhouse_s1r2_tcp_port:  Int = container.getServicePort("clickhouse-s1r2", CLICKHOUSE_TCP_PORT)
  // s2r1
  def clickhouse_s2r1_host:   String = container.getServiceHost("clickhouse-s2r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r1_http_port: Int = container.getServicePort("clickhouse-s2r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r1_grpc_port: Int = container.getServicePort("clickhouse-s2r1", CLICKHOUSE_GRPC_PORT)
  def clickhouse_s2r1_tcp_port:  Int = container.getServicePort("clickhouse-s2r1", CLICKHOUSE_TCP_PORT)
  // s2r2
  def clickhouse_s2r2_host:   String = container.getServiceHost("clickhouse-s2r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r2_http_port: Int = container.getServicePort("clickhouse-s2r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r2_grpc_port: Int = container.getServicePort("clickhouse-s2r2", CLICKHOUSE_GRPC_PORT)
  def clickhouse_s2r2_tcp_port:  Int = container.getServicePort("clickhouse-s2r2", CLICKHOUSE_TCP_PORT)
  // format: on

  override def afterStart(): Unit = {
    super.afterStart()
    // s1r1
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r1", clickhouse_s1r1_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r1_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s1r1_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r1_PORT_$CLICKHOUSE_GRPC_PORT", clickhouse_s1r1_grpc_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r1_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s1r1_tcp_port.toString))
    // s1r2
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r2", clickhouse_s1r2_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r2_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s1r2_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r2_PORT_$CLICKHOUSE_GRPC_PORT", clickhouse_s1r2_grpc_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s1r2_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s1r2_tcp_port.toString))
    // s2r1
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r1", clickhouse_s2r1_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r1_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s2r1_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r1_PORT_$CLICKHOUSE_GRPC_PORT", clickhouse_s2r1_grpc_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r1_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s2r1_tcp_port.toString))
    // s2r2
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r2", clickhouse_s2r2_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r2_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s2r2_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r2_PORT_$CLICKHOUSE_GRPC_PORT", clickhouse_s2r2_grpc_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse_s2r2_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s2r2_tcp_port.toString))
  }
}
