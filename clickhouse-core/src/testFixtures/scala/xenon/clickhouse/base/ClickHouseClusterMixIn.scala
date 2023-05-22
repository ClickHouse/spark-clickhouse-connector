/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.base

import com.clickhouse.data.ClickHouseVersion
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService, ForAllTestContainer}
import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.Utils
import xenon.clickhouse.Utils.PREFIX

import java.io.File

trait ClickHouseClusterMixIn extends AnyFunSuite with ForAllTestContainer {

  protected val ZOOKEEPER_CLIENT_PORT = 2181
  protected val CLICKHOUSE_HTTP_PORT = 8123
  protected val CLICKHOUSE_TCP_PORT = 9000

  protected val CLICKHOUSE_IMAGE: String = Utils.load(
    "CLICKHOUSE_IMAGE",
    "clickhouse/clickhouse-server:23.3"
  )

  protected val clickhouseVersion: ClickHouseVersion = ClickHouseVersion.of(CLICKHOUSE_IMAGE.split(":").last)

  test("clickhouse cluster up") {
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s1r1").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s1r1_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s1r1_PORT_$CLICKHOUSE_TCP_PORT").isDefined)

    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s1r2").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s1r2_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s1r2_PORT_$CLICKHOUSE_TCP_PORT").isDefined)

    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s2r1").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s2r1_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s2r1_PORT_$CLICKHOUSE_TCP_PORT").isDefined)

    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s2r2").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s2r2_PORT_$CLICKHOUSE_HTTP_PORT").isDefined)
    assert(sys.props.get(s"${PREFIX}_HOST_clickhouse-s2r2_PORT_$CLICKHOUSE_TCP_PORT").isDefined)
  }

  override val container: DockerComposeContainer = DockerComposeContainer.Def(
    composeFiles = new File(Utils.classpathResource("clickhouse-cluster/clickhouse-s2r2-compose.yml")),
    exposedServices = ExposedService("zookeeper", ZOOKEEPER_CLIENT_PORT) ::
      // s1r1
      ExposedService("clickhouse-s1r1", CLICKHOUSE_HTTP_PORT) ::
      ExposedService("clickhouse-s1r1", CLICKHOUSE_TCP_PORT) ::
      // s1r2
      ExposedService("clickhouse-s1r2", CLICKHOUSE_HTTP_PORT) ::
      ExposedService("clickhouse-s1r2", CLICKHOUSE_TCP_PORT) ::
      // s2r1
      ExposedService("clickhouse-s2r1", CLICKHOUSE_HTTP_PORT) ::
      ExposedService("clickhouse-s2r1", CLICKHOUSE_TCP_PORT) ::
      // s2r2
      ExposedService("clickhouse-s2r2", CLICKHOUSE_HTTP_PORT) ::
      ExposedService("clickhouse-s2r2", CLICKHOUSE_TCP_PORT) :: Nil,
    env = Map("CLICKHOUSE_IMAGE" -> CLICKHOUSE_IMAGE)
  ).createContainer()

  // format: off
  // s1r1
  def clickhouse_s1r1_host:   String = container.getServiceHost("clickhouse-s1r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r1_http_port: Int = container.getServicePort("clickhouse-s1r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r1_tcp_port:  Int = container.getServicePort("clickhouse-s1r1", CLICKHOUSE_TCP_PORT)
  // s1r2
  def clickhouse_s1r2_host:   String = container.getServiceHost("clickhouse-s1r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r2_http_port: Int = container.getServicePort("clickhouse-s1r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s1r2_tcp_port:  Int = container.getServicePort("clickhouse-s1r2", CLICKHOUSE_TCP_PORT)
  // s2r1
  def clickhouse_s2r1_host:   String = container.getServiceHost("clickhouse-s2r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r1_http_port: Int = container.getServicePort("clickhouse-s2r1", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r1_tcp_port:  Int = container.getServicePort("clickhouse-s2r1", CLICKHOUSE_TCP_PORT)
  // s2r2
  def clickhouse_s2r2_host:   String = container.getServiceHost("clickhouse-s2r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r2_http_port: Int = container.getServicePort("clickhouse-s2r2", CLICKHOUSE_HTTP_PORT)
  def clickhouse_s2r2_tcp_port:  Int = container.getServicePort("clickhouse-s2r2", CLICKHOUSE_TCP_PORT)
  // format: on

  override def afterStart(): Unit = {
    super.afterStart()
    // s1r1 http and tcp
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s1r1", clickhouse_s1r1_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s1r1_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s1r1_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s1r1_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s1r1_tcp_port.toString))
    // s1r2 http and tcp
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s1r2", clickhouse_s1r2_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s1r2_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s1r2_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s1r2_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s1r2_tcp_port.toString))
    // s2r1 http and tcp
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s2r1", clickhouse_s2r1_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s2r1_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s2r1_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s2r1_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s2r1_tcp_port.toString))
    // s2r2 http and tcp
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s2r2", clickhouse_s2r2_host))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s2r2_PORT_$CLICKHOUSE_HTTP_PORT", clickhouse_s2r2_http_port.toString))
    sys.props += ((s"${PREFIX}_HOST_clickhouse-s2r2_PORT_$CLICKHOUSE_TCP_PORT", clickhouse_s2r2_tcp_port.toString))
  }
}
