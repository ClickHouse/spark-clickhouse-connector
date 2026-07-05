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

  /** Flushes performed so far, plus the one flush that will write the pending rows. */
  def flushes(flushed: Long, pendingRows: Long): Long =
    flushed + (if (pendingRows > 0) 1 else 0)

  /**
   * Connections opened so far: one per node client in cluster mode, one in single-node mode.
   * `client` is by-name so a client is never created just to report this metric — before the
   * first flush creates one, the single connection that will write the pending rows is predicted.
   */
  def connections(flushed: Long, pendingRows: Long, client: => Either[ClusterClient, NodeClient]): Long =
    if (flushed > 0) client.fold(_.openConnections.toLong, _ => 1L)
    else if (pendingRows > 0) 1L
    else 0L
}
