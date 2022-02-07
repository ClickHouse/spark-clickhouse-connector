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

import com.dimafeng.testcontainers.{ForAllTestContainer, JdbcDatabaseContainer, SingleContainer}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.utility.{DockerImageName, MountableFile}
import xenon.clickhouse.Utils

trait ClickHouseSingleMixIn extends AnyFunSuite with ForAllTestContainer {
  // format: off
  val CLICKHOUSE_IMAGE:    String = Utils.load("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:21.8.10.19")
  val CLICKHOUSE_USER:     String = Utils.load("CLICKHOUSE_USER", "default")
  val CLICKHOUSE_PASSWORD: String = Utils.load("CLICKHOUSE_PASSWORD", "")
  val CLICKHOUSE_DB:       String = Utils.load("CLICKHOUSE_DB", "")

  private val CLICKHOUSE_HTTP_PORT = 8123
  private val CLICKHOUSE_GRPC_PORT = 9100
  private val CLICKHOUSE_TPC_PORT  = 9000
  // format: on
  override val container: SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer =
    new SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer {
      override val container: ClickHouseContainer = new ClickHouseContainer(
        // TODO: remove this workaround after https://github.com/testcontainers/testcontainers-java/pull/4925
        DockerImageName.parse(CLICKHOUSE_IMAGE).asCompatibleSubstituteFor("yandex/clickhouse-server")
      )
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
