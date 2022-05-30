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

import java.time.Duration
import java.util.{Map => JMap}

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.clickhouse.SparkOptions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object SparkOptions {
  val READ_DISTRIBUTED_USE_CLUSTER_NODES_KEY: String = "read.distributed.useClusterNodes"
  val READ_DISTRIBUTED_CONVERT_LOCAL_KEY: String = "read.distributed.convertLocal"

  val WRITE_BATCH_SIZE_KEY: String = "write.batchSize"
  val WRITE_MAX_RETRY_KEY: String = "write.maxRetry"
  val WRITE_RETRY_INTERVAL_KEY: String = "write.retryInterval"
  val WRITE_RETRYABLE_ERROR_CODES_KEY: String = "write.retryableErrorCodes"
  val WRITE_REPARTITION_NUM_KEY: String = "write.repartitionNum"
  val WRITE_REPARTITION_BY_PARTITION_KEY: String = "write.repartitionByPartition"
  val WRITE_DISTRIBUTED_USE_CLUSTER_NODES_KEY: String = "write.distributed.useClusterNodes"
  val WRITE_DISTRIBUTED_CONVERT_LOCAL_KEY: String = "write.distributed.convertLocal"
  val WRITE_LOCAL_SORT_BY_KEY_KEY: String = "write.localSortByKey"
}

trait SparkOptions extends SQLConfHelper with Serializable {
  protected def options: CaseInsensitiveStringMap

  protected def eval[T](key: String, entry: ConfigEntry[T]): T =
    Option(options.get(key)).map(entry.valueConverter).getOrElse(conf.getConf(entry))
}

class ReadOptions(_options: JMap[String, String]) extends SparkOptions {

  override protected def options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(_options)

  def useClusterNodesForDistributed: Boolean =
    eval(READ_DISTRIBUTED_USE_CLUSTER_NODES_KEY, READ_DISTRIBUTED_USE_CLUSTER_NODES)

  def convertDistributedToLocal: Boolean =
    eval(READ_DISTRIBUTED_CONVERT_LOCAL_KEY, READ_DISTRIBUTED_CONVERT_LOCAL)
}

class WriteOptions(_options: JMap[String, String]) extends SparkOptions {

  override protected def options: CaseInsensitiveStringMap = new CaseInsensitiveStringMap(_options)

  def batchSize: Int = eval(WRITE_BATCH_SIZE_KEY, WRITE_BATCH_SIZE)

  def maxRetry: Int = eval(WRITE_MAX_RETRY_KEY, WRITE_MAX_RETRY)

  def retryInterval: Duration =
    Duration.ofSeconds(eval(WRITE_RETRY_INTERVAL_KEY, WRITE_RETRY_INTERVAL))

  def retryableErrorCodes: Seq[Int] = eval(WRITE_RETRYABLE_ERROR_CODES_KEY, WRITE_RETRYABLE_ERROR_CODES)

  def repartitionNum: Int = eval(WRITE_REPARTITION_NUM_KEY, WRITE_REPARTITION_NUM)

  def repartitionByPartition: Boolean =
    eval(WRITE_REPARTITION_BY_PARTITION_KEY, WRITE_REPARTITION_BY_PARTITION)

  def useClusterNodesForDistributed: Boolean =
    eval(WRITE_DISTRIBUTED_USE_CLUSTER_NODES_KEY, WRITE_DISTRIBUTED_USE_CLUSTER_NODES)

  def convertDistributedToLocal: Boolean =
    eval(WRITE_DISTRIBUTED_CONVERT_LOCAL_KEY, WRITE_DISTRIBUTED_CONVERT_LOCAL)

  def localSortByKey: Boolean =
    eval(WRITE_LOCAL_SORT_BY_KEY_KEY, WRITE_LOCAL_SORT_BY_KEY)
}
