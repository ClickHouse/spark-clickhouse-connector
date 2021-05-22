package xenon.clickhouse

import com.dimafeng.testcontainers.{ForAllTestContainer, JdbcDatabaseContainer, SingleContainer}
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.ClickHouseContainer
import org.testcontainers.utility.MountableFile

trait ClickHouseSingleSuiteMixIn extends AnyFunSuite with ForAllTestContainer {

  val CLICKHOUSE_IMAGE: String = Utils.load("CLICKHOUSE_IMAGE", "yandex/clickhouse-server:21.3")
  val CLICKHOUSE_USER: String = Utils.load("CLICKHOUSE_USER", "default")
  val CLICKHOUSE_PASSWORD: String = Utils.load("CLICKHOUSE_PASSWORD", "")
  val CLICKHOUSE_DB: String = Utils.load("CLICKHOUSE_DB", "")

  private val CLICKHOUSE_HTTP_PORT = 8123
  private val CLICKHOUSE_GRPC_PORT = 9100
  private val CLICKHOUSE_TPC_PORT = 9000

  override val container: SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer =
    new SingleContainer[ClickHouseContainer] with JdbcDatabaseContainer {
      override val container: ClickHouseContainer = new ClickHouseContainer(CLICKHOUSE_IMAGE)
        .withEnv("CLICKHOUSE_USER", CLICKHOUSE_USER)
        .withEnv("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
        .withEnv("CLICKHOUSE_DB", CLICKHOUSE_DB)
        .withExposedPorts(CLICKHOUSE_GRPC_PORT)
        .withCopyFileToContainer(MountableFile.forClasspathResource("clickhouse-single/grpc_config.xml"), "/etc/clickhouse-server/config.d/grpc_config.xml")
        .asInstanceOf[ClickHouseContainer]
    }

  def clickhouseHost: String = container.host
  def clickhouseHttpPort: Int = container.mappedPort(CLICKHOUSE_HTTP_PORT)
  def clickhouseGrpcPort: Int = container.mappedPort(CLICKHOUSE_GRPC_PORT)
  def clickhouseTcpPort: Int = container.mappedPort(CLICKHOUSE_TPC_PORT)
}
