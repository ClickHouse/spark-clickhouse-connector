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

import com.clickhouse.client.ClickHouseVersion
import com.dimafeng.testcontainers.{ForAllTestContainer, JdbcDatabaseContainer, SingleContainer}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.utility.{DockerImageName, MountableFile}
import xenon.clickhouse.Utils

trait ClickHouseSingleMixIn extends AnyFunSuite with ForAllTestContainer {
  // format: off
  val CLICKHOUSE_IMAGE:    String = Utils.load("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:22.3.3.44")
  val CLICKHOUSE_USER:     String = Utils.load("CLICKHOUSE_USER", "default")
  val CLICKHOUSE_PASSWORD: String = Utils.load("CLICKHOUSE_PASSWORD", "")
  val CLICKHOUSE_DB:       String = Utils.load("CLICKHOUSE_DB", "")

  private val CLICKHOUSE_HTTP_PORT = 8123
  private val CLICKHOUSE_GRPC_PORT = 9100
  private val CLICKHOUSE_TPC_PORT  = 9000
  // format: on

  protected val clickhouseVersion: ClickHouseVersion = ClickHouseVersion.of(CLICKHOUSE_IMAGE.split(":").last)
  protected val grpcEnabled: Boolean = clickhouseVersion.isNewerOrEqualTo("21.1.2.15")

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
        .withExposedPorts(CLICKHOUSE_HTTP_PORT, CLICKHOUSE_GRPC_PORT, CLICKHOUSE_TPC_PORT)
        .withCopyFileToContainer(
          MountableFile.forClasspathResource("clickhouse-single/grpc_config.xml"),
          "/etc/clickhouse-server/config.d/grpc_config.xml"
        )
        .withCopyFileToContainer(
          MountableFile.forClasspathResource("clickhouse-single/users.xml"),
          "/etc/clickhouse-server/users.xml"
        )
        .asInstanceOf[ClickHouseContainer]
    }
  // format: off
  def clickhouseHost:  String = container.host
  def clickhouseHttpPort: Int = container.mappedPort(CLICKHOUSE_HTTP_PORT)
  def clickhouseGrpcPort: Int = container.mappedPort(CLICKHOUSE_GRPC_PORT)
  def clickhouseTcpPort:  Int = container.mappedPort(CLICKHOUSE_TPC_PORT)
  // format: on
}
