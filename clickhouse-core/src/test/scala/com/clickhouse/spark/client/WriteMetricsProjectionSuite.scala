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

import com.clickhouse.spark.spec.{ClusterSpec, NodeSpec, ReplicaSpec, ShardSpec}
import org.scalatest.funsuite.AnyFunSuite

class WriteMetricsProjectionSuite extends AnyFunSuite {

  test("pending rows count as written") {
    assert(WriteMetricsProjection.recordsWritten(written = 20000, pendingRows = 5000) === 25000L)
  }

  test("pending rows count as exactly one more flush") {
    assert(WriteMetricsProjection.flushes(flushed = 2, pendingRows = 5) === 3L)
    assert(WriteMetricsProjection.flushes(flushed = 2, pendingRows = 0) === 2L)
    assert(WriteMetricsProjection.flushes(flushed = 0, pendingRows = 0) === 0L)
  }

  test("batch-size folds treat 0 as no batch, not as a size") {
    assert(WriteMetricsProjection.minBatchSize(currentMin = 0, batchRows = 5) === 5L)
    assert(WriteMetricsProjection.minBatchSize(currentMin = 100, batchRows = 0) === 100L)
    assert(WriteMetricsProjection.minBatchSize(currentMin = 0, batchRows = 0) === 0L)
    assert(WriteMetricsProjection.minBatchSize(currentMin = 100, batchRows = 5) === 5L)
    assert(WriteMetricsProjection.maxBatchSize(currentMax = 100, batchRows = 5) === 100L)
  }

  test("connections are predicted without creating a client") {
    def failingClient: Either[ClusterClient, NodeClient] =
      fail("client must not be created just to report a metric")
    assert(WriteMetricsProjection.connections(flushed = 0, pendingRows = 0, failingClient) === 0L)
    assert(WriteMetricsProjection.connections(flushed = 0, pendingRows = 5, failingClient) === 1L)
  }

  test("single-node client counts as one connection without touching it") {
    assert(WriteMetricsProjection.connections(flushed = 1, pendingRows = 0, Right(null)) === 1L)
  }

  test("cluster connections count distinct (shard, replica) node clients") {
    val node = NodeSpec("127.0.0.1", Some(8123))
    val clusterSpec = ClusterSpec(
      name = "metrics-suite-cluster",
      shards = Array(
        ShardSpec(num = 1, weight = 1, replicas = Array(ReplicaSpec(1, node))),
        ShardSpec(num = 2, weight = 1, replicas = Array(ReplicaSpec(1, node)))
      )
    )
    val clusterClient = ClusterClient(clusterSpec)
    try {
      clusterClient.node(Some(1), Some(1))
      clusterClient.node(Some(1), Some(1)) // same (shard, replica) reuses the cached client
      assert(WriteMetricsProjection.connections(flushed = 1, pendingRows = 0, Left(clusterClient)) === 1L)
      clusterClient.node(Some(2), Some(1)) // a new shard opens a second client
      assert(WriteMetricsProjection.connections(flushed = 2, pendingRows = 0, Left(clusterClient)) === 2L)
    } finally clusterClient.close()
  }
}
