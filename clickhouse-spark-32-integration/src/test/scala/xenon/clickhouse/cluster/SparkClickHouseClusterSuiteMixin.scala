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

package xenon.clickhouse.cluster

import xenon.clickhouse.BaseSparkSuite
import xenon.clickhouse.base.ClickHouseClusterSuiteMixIn

trait SparkClickHouseClusterSuiteMixin {
  self: BaseSparkSuite with ClickHouseClusterSuiteMixIn =>

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[4]",
    "spark.ui.enabled" -> "false", // enable when debug
    "spark.app.name" -> "spark-clickhouse-cluster-ut",
    "spark.sql.shuffle.partitions" -> "4",
    "spark.sql.defaultCatalog" -> "clickhouse-s1r1",
    "spark.sql.catalog.clickhouse-s1r1" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s1r1.host" -> clickhouse_s1r1_host,
    "spark.sql.catalog.clickhouse-s1r1.grpc_port" -> clickhouse_s1r1_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s1r1.user" -> "default",
    "spark.sql.catalog.clickhouse-s1r1.password" -> "",
    "spark.sql.catalog.clickhouse-s1r1.database" -> "default",
    "spark.sql.catalog.clickhouse-s1r2" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s1r2.host" -> clickhouse_s1r2_host,
    "spark.sql.catalog.clickhouse-s1r2.grpc_port" -> clickhouse_s1r2_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s1r2.user" -> "default",
    "spark.sql.catalog.clickhouse-s1r2.password" -> "",
    "spark.sql.catalog.clickhouse-s1r2.database" -> "default",
    "spark.sql.catalog.clickhouse-s2r1" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s2r1.host" -> clickhouse_s2r1_host,
    "spark.sql.catalog.clickhouse-s2r1.grpc_port" -> clickhouse_s2r1_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s2r1.user" -> "default",
    "spark.sql.catalog.clickhouse-s2r1.password" -> "",
    "spark.sql.catalog.clickhouse-s2r1.database" -> "default",
    "spark.sql.catalog.clickhouse-s2r2" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse-s2r2.host" -> clickhouse_s2r2_host,
    "spark.sql.catalog.clickhouse-s2r2.grpc_port" -> clickhouse_s2r2_grpc_port.toString,
    "spark.sql.catalog.clickhouse-s2r2.user" -> "default",
    "spark.sql.catalog.clickhouse-s2r2.password" -> "",
    "spark.sql.catalog.clickhouse-s2r2.database" -> "default",
    // extended configurations
    "spark.clickhouse.write.batchSize" -> "2",
    "spark.clickhouse.write.maxRetry" -> "2",
    "spark.clickhouse.write.retryInterval" -> "1",
    "spark.clickhouse.write.retryableErrorCodes" -> "241",
    "spark.clickhouse.write.write.repartitionNum" -> "0",
    "spark.clickhouse.write.distributed.useClusterNodes" -> "true",
    "spark.clickhouse.read.distributed.useClusterNodes" -> "false",
    "spark.clickhouse.write.distributed.convertLocal" -> "false",
    "spark.clickhouse.read.distributed.convertLocal" -> "true",
    "spark.clickhouse.truncate.distributed.convertLocal" -> "true"
  )

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouse_s1r1_host,
    "grpc_port" -> clickhouse_s1r1_grpc_port.toString,
    "user" -> "default",
    "password" -> "",
    "database" -> "default"
  )
}
