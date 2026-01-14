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

import com.clickhouse.data.ClickHouseCompression
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.Duration
import java.util.{Map => JMap}

trait SparkOptions extends SQLConfHelper with Serializable {
  protected def options: CaseInsensitiveStringMap

  protected def eval[T](key: String, entry: ConfigEntry[T]): T =
    Option(options.get(key)).map(entry.valueConverter).getOrElse(conf.getConf(entry))
}

class ReadOptions(_options: JMap[String, String]) extends SparkOptions {

  override protected def options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(_options)

  def useClusterNodesForDistributed: Boolean =
    eval(READ_DISTRIBUTED_USE_CLUSTER_NODES.key, READ_DISTRIBUTED_USE_CLUSTER_NODES)

  def convertDistributedToLocal: Boolean =
    eval(READ_DISTRIBUTED_CONVERT_LOCAL.key, READ_DISTRIBUTED_CONVERT_LOCAL)

  def splitByPartitionId: Boolean =
    eval(READ_SPLIT_BY_PARTITION_ID.key, READ_SPLIT_BY_PARTITION_ID)

  def compressionCodec: ClickHouseCompression =
    ClickHouseCompression.fromEncoding(eval(READ_COMPRESSION_CODEC.key, READ_COMPRESSION_CODEC))

  def format: String =
    eval(READ_FORMAT.key, READ_FORMAT)

  def runtimeFilterEnabled: Boolean =
    eval(RUNTIME_FILTER_ENABLED.key, RUNTIME_FILTER_ENABLED)
}

class WriteOptions(_options: JMap[String, String]) extends SparkOptions {

  override protected def options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(_options)

  def batchSize: Int = eval(WRITE_BATCH_SIZE.key, WRITE_BATCH_SIZE)

  def maxRetry: Int = eval(WRITE_MAX_RETRY.key, WRITE_MAX_RETRY)

  def retryInterval: Duration =
    Duration.ofSeconds(eval(WRITE_RETRY_INTERVAL.key, WRITE_RETRY_INTERVAL))

  def retryableErrorCodes: Seq[Int] = eval(WRITE_RETRYABLE_ERROR_CODES.key, WRITE_RETRYABLE_ERROR_CODES)

  def repartitionNum: Int = eval(WRITE_REPARTITION_NUM.key, WRITE_REPARTITION_NUM)

  def repartitionByPartition: Boolean =
    eval(WRITE_REPARTITION_BY_PARTITION.key, WRITE_REPARTITION_BY_PARTITION)

  def repartitionStrictly: Boolean =
    eval(WRITE_REPARTITION_STRICTLY.key, WRITE_REPARTITION_STRICTLY)

  def useClusterNodesForDistributed: Boolean =
    eval(WRITE_DISTRIBUTED_USE_CLUSTER_NODES.key, WRITE_DISTRIBUTED_USE_CLUSTER_NODES)

  def convertDistributedToLocal: Boolean =
    eval(WRITE_DISTRIBUTED_CONVERT_LOCAL.key, WRITE_DISTRIBUTED_CONVERT_LOCAL)

  def allowUnsupportedShardingWithConvertLocal: Boolean =
    eval(WRITE_DISTRIBUTED_CONVERT_LOCAL_ALLOW_UNSUPPORTED_SHARDING.key, WRITE_DISTRIBUTED_CONVERT_LOCAL_ALLOW_UNSUPPORTED_SHARDING)

  def localSortByPartition: Boolean =
    eval(WRITE_LOCAL_SORT_BY_PARTITION.key, WRITE_LOCAL_SORT_BY_PARTITION)

  def localSortByKey: Boolean =
    eval(WRITE_LOCAL_SORT_BY_KEY.key, WRITE_LOCAL_SORT_BY_KEY)

  def compressionCodec: ClickHouseCompression =
    ClickHouseCompression.fromEncoding(eval(WRITE_COMPRESSION_CODEC.key, WRITE_COMPRESSION_CODEC))

  def format: String =
    eval(WRITE_FORMAT.key, WRITE_FORMAT)
}
