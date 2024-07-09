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

import com.clickhouse.client.ClickHouseProtocol
import com.clickhouse.client.ClickHouseProtocol._
import com.clickhouse.data.ClickHouseVersion
import com.dimafeng.testcontainers.{ForAllTestContainer, JdbcDatabaseContainer, SingleContainer}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.utility.{DockerImageName, MountableFile}
import xenon.clickhouse.Utils
import xenon.clickhouse.client.NodeClient
import xenon.clickhouse.spec.NodeSpec

import java.nio.file.{Path, Paths}

trait ClickHouseSingleMixIn extends AnyFunSuite with ForAllTestContainer {
  // format: off
  val CLICKHOUSE_VERSION:  String = Utils.load("CLICKHOUSE_VERSION", "23.8")
  val isCloud: Boolean = if (CLICKHOUSE_VERSION.equalsIgnoreCase("cloud")) true else false
  val CLICKHOUSE_IMAGE:    String = Utils.load("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:23.8")
  val CLICKHOUSE_USER:     String = Utils.load("CLICKHOUSE_USER", "default")
  val CLICKHOUSE_PASSWORD: String = Utils.load("CLICKHOUSE_PASSWORD", "")
  val CLICKHOUSE_DB:       String = Utils.load("CLICKHOUSE_DB", "")

  private val CLICKHOUSE_HTTP_PORT = 8123
  private val CLICKHOUSE_TPC_PORT  = 9000
  private val CLICKHOUSE_CLOUD_HTTP_PORT = 8443
  private val CLICKHOUSE_CLOUD_TCP_PORT = 9000
  // format: on

  protected val clickhouseVersion: ClickHouseVersion = ClickHouseVersion.of(CLICKHOUSE_IMAGE.split(":").last)

  protected val rootProjectDir: Path = {
    val thisClassURI = this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
    val currentPath = Paths.get(thisClassURI).toAbsolutePath.normalize
    val coreModuleIndex = currentPath.toString.indexOf("/clickhouse-core")
    if (coreModuleIndex > 0) {
      Paths.get(currentPath.toString.substring(0, coreModuleIndex))
    } else {
      val sparkModuleIndex = currentPath.toString.indexOf("/clickhouse-spark")
      require(sparkModuleIndex > 0, s"illegal path: $currentPath")
      Paths.get(currentPath.toString.substring(0, sparkModuleIndex)).getParent
    }
  }

  override val container: SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer =
    new SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer {
      override val container: ClickHouseContainer = new ClickHouseContainer(
        DockerImageName.parse(CLICKHOUSE_IMAGE).asCompatibleSubstituteFor("clickhouse/clickhouse-server")
      ) {
        // TODO: remove this workaround after https://github.com/testcontainers/testcontainers-java/pull/5666
        override def getDriverClassName: String = "com.clickhouse.jdbc.ClickHouseDriver"
      }
        .withEnv("CLICKHOUSE_USER", CLICKHOUSE_USER)
        .withEnv("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
        .withEnv("CLICKHOUSE_DB", CLICKHOUSE_DB)
        .withExposedPorts(CLICKHOUSE_HTTP_PORT, CLICKHOUSE_TPC_PORT)
        .withFileSystemBind(s"${sys.env("ROOT_PROJECT_DIR")}/log/clickhouse-server", "/var/log/clickhouse-server")
        .withCopyFileToContainer(
          MountableFile.forClasspathResource("clickhouse-single/users.xml"),
          "/etc/clickhouse-server/users.xml"
        )
        .asInstanceOf[ClickHouseContainer]
    }
  // format: off
  def clickhouseHost:  String = if (isCloud) sys.env.get("INTEGRATIONS_TEAM_TESTS_CLOUD_HOST_SMT").get else container.host
  def clickhouseHttpPort: Int = if (isCloud) CLICKHOUSE_CLOUD_HTTP_PORT else container.mappedPort(CLICKHOUSE_HTTP_PORT)
  def clickhouseTcpPort:  Int = if (isCloud) CLICKHOUSE_CLOUD_TCP_PORT else container.mappedPort(CLICKHOUSE_TPC_PORT)
  def clickhousePassword: String = if (isCloud) sys.env.get("INTEGRATIONS_TEAM_TESTS_CLOUD_PASSWORD_SMT").get else CLICKHOUSE_PASSWORD

  // format: on
  def withNodeClient(protocol: ClickHouseProtocol = HTTP)(block: NodeClient => Unit): Unit =
    Utils.tryWithResource {
      NodeClient(NodeSpec(
        container.host,
        Some(container.mappedPort(CLICKHOUSE_HTTP_PORT)),
        Some(container.mappedPort(CLICKHOUSE_TPC_PORT)),
        protocol
      ))
    } {
      client => block(client)
    }
}
