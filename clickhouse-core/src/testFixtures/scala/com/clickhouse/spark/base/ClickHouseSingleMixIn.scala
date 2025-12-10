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

package com.clickhouse.spark.base

import com.clickhouse.spark.Utils
import com.clickhouse.data.ClickHouseVersion
import com.dimafeng.testcontainers.{ForAllTestContainer, JdbcDatabaseContainer, SingleContainer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.containers.BindMode
import org.testcontainers.utility.{DockerImageName, MountableFile}
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._

trait ClickHouseSingleMixIn extends AnyFunSuite with BeforeAndAfterAll with ForAllTestContainer
    with ClickHouseProvider {

  private val logger = LoggerFactory.getLogger(getClass)
  // format: off
  private val CLICKHOUSE_IMAGE:    String = Utils.load("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:23.8")
  private val CLICKHOUSE_USER:     String = Utils.load("CLICKHOUSE_USER", "default")
  private val CLICKHOUSE_PASSWORD: String = Utils.load("CLICKHOUSE_PASSWORD", "")
  private val CLICKHOUSE_DB:       String = Utils.load("CLICKHOUSE_DB", "")

  private val CLICKHOUSE_HTTP_PORT = 8123
  private val CLICKHOUSE_TPC_PORT  = 9000
  // format: on

  logger.info(s"Initializing with ClickHouse image: $CLICKHOUSE_IMAGE")

  override val clickhouseVersion: ClickHouseVersion = ClickHouseVersion.of(CLICKHOUSE_IMAGE.split(":").last)

  protected val rootProjectDir: Path = {
    val thisClassURI = this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI
    val currentPath = Paths.get(thisClassURI).toAbsolutePath.normalize
    val eachFolder = currentPath.iterator().asScala.toIndexedSeq
    val coreModuleIndex = eachFolder.indexWhere(_.toString.startsWith("clickhouse-core"))
    val sparkModuleIndex = eachFolder.indexWhere(_.toString.startsWith("clickhouse-spark"))
    require(coreModuleIndex > 0 || sparkModuleIndex > 0, s"illegal path: $currentPath")
    if (coreModuleIndex > 0) {
      eachFolder.take(coreModuleIndex).reduce((acc, i) => acc.resolve(i))
    } else if (sparkModuleIndex > 0) {
      eachFolder.take(sparkModuleIndex).dropRight(1).reduce((acc, i) => acc.resolve(i))
    } else { // unreachable code
      throw new IllegalArgumentException(s"illegal path: $currentPath")
    }
  }

  override val container: SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer =
    new SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer {
      override val container: ClickHouseContainer = {
        val logPath = rootProjectDir.resolve("log/clickhouse-server").toString
        val c = new ClickHouseContainer(
          DockerImageName.parse(CLICKHOUSE_IMAGE).asCompatibleSubstituteFor("clickhouse/clickhouse-server")
        )
        // Configure with empty password to match users.xml
        c.withUsername(CLICKHOUSE_USER)
        c.withPassword(CLICKHOUSE_PASSWORD)
        if (!CLICKHOUSE_DB.isEmpty) {
          c.withDatabaseName(CLICKHOUSE_DB)
        }
        c.withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
          .withExposedPorts(CLICKHOUSE_HTTP_PORT, CLICKHOUSE_TPC_PORT)
          .withFileSystemBind(logPath, "/var/log/clickhouse-server", BindMode.READ_WRITE)
          .withCopyFileToContainer(
            MountableFile.forClasspathResource("clickhouse-single/users.xml"),
            "/etc/clickhouse-server/users.d/users.xml"
          )
        c
      }
    }

  override def clickhouseHost: String = container.host
  override def clickhouseHttpPort: Int = container.mappedPort(CLICKHOUSE_HTTP_PORT)
  override def clickhouseTcpPort: Int = container.mappedPort(CLICKHOUSE_TPC_PORT)
  override def clickhouseUser: String = CLICKHOUSE_USER
  override def clickhousePassword: String = CLICKHOUSE_PASSWORD
  override def clickhouseDatabase: String = CLICKHOUSE_DB
  override def isSslEnabled: Boolean = false

  override def beforeAll(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Starting ClickHouse container: $CLICKHOUSE_IMAGE")
    super.beforeAll() // This starts the container and makes mappedPort available
    val duration = System.currentTimeMillis() - startTime
    logger.info(
      s"ClickHouse container started in ${duration}ms at ${container.host}:${container.mappedPort(CLICKHOUSE_HTTP_PORT)}"
    )
  }

  override def afterAll(): Unit = {
    logger.info("Stopping ClickHouse container")
    super.afterAll()
    logger.info("ClickHouse container stopped")
  }
}
