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

import org.scalatest.funsuite.AnyFunSuite

class WriteMetricsProjectionSuite extends AnyFunSuite {

  test("pending rows count as one more flush") {
    assert(WriteMetricsProjection.flushes(flushed = 2, pendingRows = 5) === 3L)
    assert(WriteMetricsProjection.flushes(flushed = 2, pendingRows = 0) === 2L)
    assert(WriteMetricsProjection.flushes(flushed = 0, pendingRows = 0) === 0L)
  }

  test("batches fold into running min/max batch size") {
    assert(WriteMetricsProjection.minBatchSize(currentMin = 100, batchRows = 5) === 5L)
    assert(WriteMetricsProjection.minBatchSize(currentMin = 100, batchRows = 0) === 100L)
    assert(WriteMetricsProjection.minBatchSize(currentMin = 0, batchRows = 5) === 5L)
    assert(WriteMetricsProjection.minBatchSize(currentMin = 0, batchRows = 0) === 0L)
    assert(WriteMetricsProjection.maxBatchSize(currentMax = 100, batchRows = 5) === 100L)
    assert(WriteMetricsProjection.maxBatchSize(currentMax = 0, batchRows = 5) === 5L)
  }

  test("connections are predicted without creating a client") {
    def failingClient: Either[ClusterClient, NodeClient] =
      fail("client must not be created just to report a metric")
    assert(WriteMetricsProjection.connections(flushed = 0, pendingRows = 0, failingClient) === 0L)
    assert(WriteMetricsProjection.connections(flushed = 0, pendingRows = 5, failingClient) === 1L)
  }

  test("connections count the client once a flush has created it") {
    assert(WriteMetricsProjection.connections(flushed = 1, pendingRows = 0, Right(null)) === 1L)
  }
}
