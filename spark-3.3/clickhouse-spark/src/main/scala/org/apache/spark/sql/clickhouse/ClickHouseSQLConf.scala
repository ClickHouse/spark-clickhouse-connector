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

import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.spark.sql.internal.SQLConf._
import xenon.clickhouse.exception.ClickHouseErrCode._

import java.util.concurrent.TimeUnit

object ClickHouseSQLConf {

  val WRITE_BATCH_SIZE: ConfigEntry[Int] =
    buildConf("spark.clickhouse.write.batchSize")
      .doc("The number of records per batch on writing to ClickHouse.")
      .version("0.1.0")
      .intConf
      .checkValue(v => v > 0 && v <= 1000000, "Should be positive but less than or equals 1000000.")
      .createWithDefault(10000)

  val WRITE_MAX_RETRY: ConfigEntry[Int] =
    buildConf("spark.clickhouse.write.maxRetry")
      .doc("The maximum number of write we will retry for a single batch write failed with retryable codes.")
      .version("0.1.0")
      .intConf
      .checkValue(_ >= 0, "Should be 0 or positive value. 0 means disable retry.")
      .createWithDefault(3)

  val WRITE_RETRY_INTERVAL: ConfigEntry[Long] =
    buildConf("spark.clickhouse.write.retryInterval")
      .doc("The interval in seconds between write retry.")
      .version("0.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(10)

  val WRITE_RETRYABLE_ERROR_CODES: ConfigEntry[Seq[Int]] =
    buildConf("spark.clickhouse.write.retryableErrorCodes")
      .doc("The retryable error codes returned by ClickHouse server when write failing.")
      .version("0.1.0")
      .intConf
      .toSequence
      .checkValue(codes => !codes.exists(_ <= OK.code), "Error code should be positive.")
      .createWithDefault(MEMORY_LIMIT_EXCEEDED.code :: Nil)

  val WRITE_REPARTITION_NUM: ConfigEntry[Int] =
    buildConf("spark.clickhouse.write.repartitionNum")
      .doc("Repartition data to meet the distributions of ClickHouse table is required before writing, " +
        "use this conf to specific the repartition number, value less than 1 mean no requirement.")
      .version("0.1.0")
      .intConf
      .createWithDefault(0)

  val WRITE_REPARTITION_BY_PARTITION: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.write.repartitionByPartition")
      .doc("Whether to repartition data by ClickHouse partition keys to meet the distributions of " +
        "ClickHouse table before writing.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val WRITE_REPARTITION_STRICTLY: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.write.repartitionStrictly")
      .doc("If true, Spark will strictly distribute incoming records across partitions to satisfy " +
        "the required distribution before passing the records to the data source table on write. " +
        "Otherwise, Spark may apply certain optimizations to speed up the query but break the " +
        "distribution requirement. Note, this configuration requires SPARK-37523, w/o this patch, " +
        "it always act as `true`.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val WRITE_DISTRIBUTED_USE_CLUSTER_NODES: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.write.distributed.useClusterNodes")
      .doc("Write to all nodes of cluster when writing Distributed table.")
      .version("0.1.0")
      .booleanConf
      .createWithDefault(true)

  val READ_DISTRIBUTED_USE_CLUSTER_NODES: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.read.distributed.useClusterNodes")
      .doc("Read from all nodes of cluster when reading Distributed table.")
      .version("0.1.0")
      .booleanConf
      .checkValue(_ == false, s"`spark.clickhouse.read.distributed.useClusterNodes` is not support yet.")
      .createWithDefault(false)

  val WRITE_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.write.distributed.convertLocal")
      .doc("When writing Distributed table, write local table instead of itself. " +
        "If `true`, ignore `spark.clickhouse.write.distributed.useClusterNodes`.")
      .version("0.1.0")
      .booleanConf
      .createWithDefault(false)

  val READ_DISTRIBUTED_CONVERT_LOCAL: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.read.distributed.convertLocal")
      .doc("When reading Distributed table, read local table instead of itself. " +
        s"If `true`, ignore `${READ_DISTRIBUTED_USE_CLUSTER_NODES.key}`.")
      .version("0.1.0")
      .booleanConf
      .createWithDefault(true)

  val WRITE_LOCAL_SORT_BY_PARTITION: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.write.localSortByPartition")
      .doc("If `true`, do local sort by partition before writing.")
      .version("0.3.0")
      .fallbackConf(WRITE_REPARTITION_BY_PARTITION)

  val WRITE_LOCAL_SORT_BY_KEY: ConfigEntry[Boolean] =
    buildConf("spark.clickhouse.write.localSortByKey")
      .doc("If `true`, do local sort by sort keys before writing.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val WRITE_COMPRESSION_CODEC: ConfigEntry[String] =
    buildConf("spark.clickhouse.write.compression.codec")
      .doc("The codec used to compress data for writing. Supported codecs: none, gzip, lz4, zstd")
      .version("0.3.0")
      .stringConf
      .createWithDefault("zstd")

  val WRITE_COMPRESSION_ZSTD_LEVEL: ConfigEntry[Int] =
    buildConf("spark.clickhouse.write.compression.zstd.level")
      .doc("ZStd compression level")
      .version("0.4.0")
      .intConf
      .createWithDefault(3)

  val WRITE_COMPRESSION_ZSTD_THREAD: ConfigEntry[Int] =
    buildConf("spark.clickhouse.write.compression.zstd.thread")
      .doc("ZStd worker pool size. Default value is `0`, aka 'single-threaded mode': no worker is spawned. " +
        "Spawn worker threads and trigger asynchronous mode when value equals or greater than 1.")
      .version("0.4.0")
      .intConf
      .createWithDefault(0)

  val WRITE_FORMAT: ConfigEntry[String] =
    buildConf("spark.clickhouse.write.format")
      .doc("Serialize format for writing. Supported formats: JSONEachRow, ArrowStream")
      .version("0.3.0")
      .stringConf
      .createWithDefault("ArrowStream")
}
