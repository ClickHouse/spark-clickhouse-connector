package org.apache.spark.sql.clickhouse

import java.time.Duration

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.clickhouse.WriteOptions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object WriteOptions {
  val WRITE_BATCH_SIZE_KEY: String = "write.batchSize"
  val WRITE_MAX_RETRY_KEY: String = "write.maxRetry"
  val WRITE_RETRY_INTERVAL_KEY: String = "write.retryInterval"
  val WRITE_RETRYABLE_ERROR_CODES_KEY: String = "write.retryableErrorCodes"
  val WRITE_REPARTITION_NUM_KEY: String = "write.repartitionNum"
  val WRITE_DISTRIBUTED_USE_CLUSTER_NODES_KEY: String = "write.distributed.useClusterNodes"
  val WRITE_DISTRIBUTED_CONVERT_LOCAL_KEY: String = "write.distributed.convertLocal"
}

class WriteOptions(options: CaseInsensitiveStringMap) extends SQLConfHelper {

  def batchSize: Int = eval(WRITE_BATCH_SIZE_KEY, WRITE_BATCH_SIZE)

  def maxRetry: Int = eval(WRITE_MAX_RETRY_KEY, WRITE_MAX_RETRY)

  def retryInterval: Duration =
    Duration.ofSeconds(eval(WRITE_RETRY_INTERVAL_KEY, WRITE_RETRY_INTERVAL))

  def retryableErrorCodes: Seq[Int] = eval(WRITE_RETRYABLE_ERROR_CODES_KEY, WRITE_RETRYABLE_ERROR_CODES)

  def repartitionNum: Int = eval(WRITE_REPARTITION_NUM_KEY, WRITE_REPARTITION_NUM)

  def useClusterNodesForDistributed: Boolean =
    eval(WRITE_DISTRIBUTED_USE_CLUSTER_NODES_KEY, WRITE_DISTRIBUTED_USE_CLUSTER_NODES)

  def convertDistributedToLocal: Boolean =
    eval(WRITE_DISTRIBUTED_CONVERT_LOCAL_KEY, WRITE_DISTRIBUTED_CONVERT_LOCAL)

  private def eval[T](key: String, entry: ConfigEntry[T]): T =
    Option(options.get(key)).map(entry.valueConverter).getOrElse(conf.getConf(entry))
}
