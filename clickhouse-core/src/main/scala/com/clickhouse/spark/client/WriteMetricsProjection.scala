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

package com.clickhouse.spark.client

/**
 * Write-task metric projections. Task metrics are snapshotted while the last batch may still be
 * buffered (it is flushed during commit), so pending rows are counted as the flush and the
 * connection that will write them.
 */
object WriteMetricsProjection {

  /** Rows written so far, plus the pending rows that commit() will write. */
  def recordsWritten(written: Long, pendingRows: Long): Long =
    written + pendingRows

  /** Flushes performed so far, plus the one flush that will write the pending rows. */
  def flushes(flushed: Long, pendingRows: Long): Long =
    flushed + (if (pendingRows > 0) 1 else 0)

  /** Folds a batch (flushed, or still pending) into a running minimum batch size, where 0 means "no batch". */
  def minBatchSize(currentMin: Long, batchRows: Long): Long =
    Seq(currentMin, batchRows).filter(_ > 0).reduceOption(_ min _).getOrElse(0L)

  /** Folds a batch (flushed, or still pending) into a running maximum batch size. */
  def maxBatchSize(currentMax: Long, batchRows: Long): Long =
    currentMax.max(batchRows)

  /** Which quarter-fill bucket (0-3) a batch belongs to, relative to the configured batch size. */
  def batchFillBucket(batchRows: Long, batchSize: Long): Int =
    ((batchRows - 1) * 4 / batchSize).toInt.max(0).min(3)

  /** Batches counted in a fill bucket so far, plus the pending batch if it belongs to that bucket. */
  def bucketedBatches(counted: Long, bucket: Int, pendingRows: Long, batchSize: Long): Long =
    counted + (if (pendingRows > 0 && batchFillBucket(pendingRows, batchSize) == bucket) 1 else 0)

  /**
   * Connections opened so far: one per node client in cluster mode, one in single-node mode.
   * `client` is by-name so a client is never created just to report this metric — before the
   * first flush creates one, the single connection that will write the pending rows is predicted.
   */
  def connections(flushed: Long, pendingRows: Long, client: => Either[ClusterClient, NodeClient]): Long = {
    val clientCreated = flushed > 0 // reading `client` before the first flush would create it
    if (clientCreated) countOpenConnections(client)
    else if (pendingRows > 0) 1L // commit() will open the first connection for the pending batch
    else 0L
  }

  private def countOpenConnections(client: Either[ClusterClient, NodeClient]): Long = client match {
    case Left(clusterClient) => clusterClient.openConnections.toLong // cluster: one per (shard, replica) used
    case Right(_) => 1L // single node: exactly one
  }
}
