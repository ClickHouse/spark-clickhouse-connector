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

package org.apache.spark.sql.clickhouse

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.sql.clickhouse.SparkOptions._
import org.apache.spark.sql.internal.SQLConf
import xenon.clickhouse.exception.ClickHouseErrCode._

object ClickHouseSQLConf {
  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.clickhouse.$key")
  def buildStaticConf(key: String): ConfigBuilder = SQLConf.buildStaticConf(s"spark.clickhouse.$key")

  val WRITE_BATCH_SIZE: ConfigEntry[Int] =
    buildConf(WRITE_BATCH_SIZE_KEY)
      .doc("The number of records per batch on writing to ClickHouse.")
      .version("0.1.0")
      .intConf
      .checkValue(v => v > 0 && v <= 100000, "Should be positive but less than or equals 100000.")
      .createWithDefault(10000)

  val WRITE_MAX_RETRY: ConfigEntry[Int] =
    buildConf(WRITE_MAX_RETRY_KEY)
      .doc("The maximum number of write we will retry for a single batch write failed with retryable codes.")
      .version("0.1.0")
      .intConf
      .checkValue(_ >= 0, "Should be 0 or positive value. 0 means disable retry.")
      .createWithDefault(3)

  val WRITE_RETRY_INTERVAL: ConfigEntry[Long] =
    buildConf(WRITE_RETRY_INTERVAL_KEY)
      .doc("The interval in seconds between write retry.")
      .version("0.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val WRITE_RETRYABLE_ERROR_CODES: ConfigEntry[Seq[Int]] =
    buildConf(WRITE_RETRYABLE_ERROR_CODES_KEY)
      .doc("The retryable error codes returned by ClickHouse server when write failing.")
      .version("0.1.0")
      .intConf
      .toSequence
      .checkValue(codes => !codes.exists(_ <= OK.code), "Error code should be positive.")
      .createWithDefault(MEMORY_LIMIT_EXCEEDED.code :: Nil)

  val WRITE_REPARTITION_NUM: ConfigEntry[Int] =
    buildConf(WRITE_REPARTITION_NUM_KEY)
      .doc("Repartition data to meet the distributions of ClickHouse table is required before writing, " +
        "use this conf to specific the repartition number, value less than 1 mean no requirement.")
      .version("0.1.0")
      .intConf
      .createWithDefault(0)

  val WRITE_REPARTITION_BY_PARTITION: ConfigEntry[Boolean] =
    buildConf(WRITE_REPARTITION_BY_PARTITION_KEY)
      .doc("Whether to repartition data by ClickHouse partition keys to meet the distributions of " +
        "ClickHouse table before writing.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val WRITE_DISTRIBUTED_USE_CLUSTER_NODES: ConfigEntry[Boolean] =
    buildConf(WRITE_DISTRIBUTED_USE_CLUSTER_NODES_KEY)
      .doc("Write to all nodes of cluster when writing Distributed table.")
      .version("0.1.0")
      .booleanConf
      .createWithDefault(true)

  val READ_DISTRIBUTED_USE_CLUSTER_NODES: ConfigEntry[Boolean] =
    buildConf(READ_DISTRIBUTED_USE_CLUSTER_NODES_KEY)
      .doc("Read from all nodes of cluster when reading Distributed table.")
      .version("0.1.0")
      .booleanConf
      .checkValue(_ == false, s"`$READ_DISTRIBUTED_USE_CLUSTER_NODES_KEY` is not support yet.")
      .createWithDefault(false)

  val WRITE_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf(WRITE_DISTRIBUTED_CONVERT_LOCAL_KEY)
      .doc("When writing Distributed table, write local table instead of itself. " +
        s"If `true`, ignore `$WRITE_DISTRIBUTED_USE_CLUSTER_NODES_KEY`.")
      .version("0.1.0")
      .booleanConf
      .createWithDefault(false)

  val READ_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf(READ_DISTRIBUTED_CONVERT_LOCAL_KEY)
      .doc("When reading Distributed table, read local table instead of itself. " +
        s"If `true`, ignore `$READ_DISTRIBUTED_USE_CLUSTER_NODES_KEY`.")
      .version("0.1.0")
      .booleanConf
      .createWithDefault(true)

  val WRITE_LOCAL_SORT_BY_PARTITION: ConfigEntry[Boolean] =
    buildConf(WRITE_LOCAL_SORT_BY_PARTITION_KEY)
      .doc("If `true`, do local sort by partition before writing.")
      .version("0.3.0")
      .fallbackConf(WRITE_REPARTITION_BY_PARTITION)

  val WRITE_LOCAL_SORT_BY_KEY: ConfigEntry[Boolean] =
    buildConf(WRITE_LOCAL_SORT_BY_KEY_KEY)
      .doc("If `true`, do local sort by sort keys before writing.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)
}
