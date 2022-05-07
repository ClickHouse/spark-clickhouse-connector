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

package xenon.clickhouse.single

import xenon.clickhouse.BaseSparkSuite
import xenon.clickhouse.base.ClickHouseSingleMixIn

trait SparkClickHouseSingleMixin {
  self: BaseSparkSuite with ClickHouseSingleMixIn =>

  override def sparkOptions: Map[String, String] = Map(
    "spark.master" -> "local[2]",
    "spark.ui.enabled" -> "false", // enable when debug
    "spark.sql.codegen.wholeStage" -> "false",
    "spark.app.name" -> "spark-clickhouse-single-ut",
    "spark.sql.shuffle.partitions" -> "2",
    "spark.sql.defaultCatalog" -> "clickhouse",
    "spark.sql.catalog.clickhouse" -> "xenon.clickhouse.ClickHouseCatalog",
    "spark.sql.catalog.clickhouse.host" -> clickhouseHost,
    "spark.sql.catalog.clickhouse.grpc_port" -> clickhouseGrpcPort.toString,
    "spark.sql.catalog.clickhouse.user" -> CLICKHOUSE_USER,
    "spark.sql.catalog.clickhouse.password" -> CLICKHOUSE_PASSWORD,
    "spark.sql.catalog.clickhouse.database" -> CLICKHOUSE_DB,
    // extended configurations
    "spark.clickhouse.write.batchSize" -> "2",
    "spark.clickhouse.write.maxRetry" -> "2",
    "spark.clickhouse.write.retryInterval" -> "1",
    "spark.clickhouse.write.retryableErrorCodes" -> "241",
    "spark.clickhouse.write.write.repartitionNum" -> "0"
  )

  override def cmdRunnerOptions: Map[String, String] = Map(
    "host" -> clickhouseHost,
    "grpc_port" -> clickhouseGrpcPort.toString,
    "user" -> CLICKHOUSE_USER,
    "password" -> CLICKHOUSE_PASSWORD,
    "database" -> CLICKHOUSE_DB
  )
}
