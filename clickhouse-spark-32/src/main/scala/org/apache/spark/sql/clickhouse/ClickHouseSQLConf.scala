/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.clickhouse

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.sql.internal.SQLConf
import xenon.clickhouse.exception.ClickHouseErrCode._

object ClickHouseSQLConf extends ClickHouseSQLConfBase

trait ClickHouseSQLConfBase {
  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.clickhouse.$key")
  def buildStaticConf(key: String): ConfigBuilder = SQLConf.buildStaticConf(s"spark.clickhouse.$key")

  val WRITE_BATCH_SIZE: ConfigEntry[Int] =
    buildConf("write.batchSize")
      .doc("The number of records per batch on writing to ClickHouse.")
      .intConf
      .checkValue(v => v > 0 && v <= 100000, "Should be positive but less than or equals 100000.")
      .createWithDefault(50000)

  val WRITE_MAX_RETRY: ConfigEntry[Int] =
    buildConf("write.maxRetry")
      .doc("The maximum number of write we will retry for a single batch write failed with retryable codes.")
      .intConf
      .checkValue(_ >= 0, "Should be 0 or positive value. 0 means disable retry.")
      .createWithDefault(3)

  val WRITE_RETRY_INTERVAL: ConfigEntry[Long] =
    buildConf("write.retryInterval")
      .doc("The interval in seconds between write retry.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val WRITE_RETRYABLE_ERROR_CODES: ConfigEntry[Seq[Int]] =
    buildConf("write.retryableErrorCodes")
      .doc("The retryable error codes returned by ClickHouse server when write failing.")
      .intConf
      .toSequence
      .checkValue(codes => !codes.exists(_ <= OK.code), "Error code should be positive.")
      .createWithDefault(MEMORY_LIMIT_EXCEEDED.code :: Nil)

  val WRITE_REPARTITION_NUM: ConfigEntry[Int] =
    buildConf("write.repartitionNum")
      .doc("Repartition data to meet the distributions of ClickHouse table is required before writing, " +
        "use this conf to specific the repartition number, value less than 1 mean no requirement.")
      .intConf
      .createWithDefault(0)

  val WRITE_DISTRIBUTED_USE_CLUSTER_NODES: ConfigEntry[Boolean] =
    buildConf("write.distributed.useClusterNodes")
      .doc("Write to all nodes of cluster when writing Distributed table.")
      .booleanConf
      .createWithDefault(true)

  val READ_DISTRIBUTED_USE_CLUSTER_NODES: ConfigEntry[Boolean] =
    buildConf("read.distributed.useClusterNodes")
      .doc("Read from all nodes of cluster when reading Distributed table.")
      .booleanConf
      .checkValue(_ == false, "`read.distributed.useClusterNodes` is not support yet.")
      .createWithDefault(false)

  val WRITE_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf("write.distributed.convertLocal")
      .doc("When writing Distributed table, write local table instead of itself. " +
        "If `true`, ignore `write.distributed.useClusterNodes`.")
      .booleanConf
      .createWithDefault(false)

  val READ_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf("read.distributed.convertLocal")
      .doc("When reading Distributed table, read local table instead of itself. " +
        "If `true`, ignore `read.distributed.useClusterNodes`.")
      .booleanConf
      .createWithDefault(true)

  val TRUNCATE_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf("truncate.distributed.convertLocal")
      .doc("When truncate Distributed table, truncate local table instead of itself.")
      .booleanConf
      .createWithDefault(true)
}
