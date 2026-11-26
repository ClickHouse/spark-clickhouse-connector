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
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * Offline tests for sticky-client eviction and dead-replica skipping. Building a [[NodeClient]]
 * does not open a connection.
 */
class ClusterClientFailoverSuite extends AnyFunSuite with BeforeAndAfterEach {

  private def node(host: String): NodeSpec =
    NodeSpec(host, _http_port = Some(8123), _tcp_port = Some(9000))

  private val n1 = node("s1r1")
  private val n2 = node("s1r2")
  private val shard1 = ShardSpec(1, 1, Array(ReplicaSpec(1, n1), ReplicaSpec(2, n2)))
  private val testCluster = ClusterSpec("test", Array(shard1))

  override def afterEach(): Unit = NodeHealthTracker.clear()

  test("node skips replicas marked dead") {
    val client = ClusterClient(testCluster)
    try {
      NodeHealthTracker.markDead(n1)
      val hosts = (1 to 20).map { _ =>
        client.cache.clear()
        client.node(Some(1)).nodeSpec.host
      }.toSet
      assert(hosts === Set(n2.host))
    } finally
      client.close()
  }

  test("failover evicts sticky client and picks another replica") {
    val client = ClusterClient(testCluster)
    try {
      val first = client.node(Some(1))
      val firstHost = first.nodeSpec.host
      client.failover(Some(1))
      assert(NodeHealthTracker.isDead(first.nodeSpec))
      val hosts = (1 to 10).map { _ =>
        client.cache.clear()
        client.node(Some(1)).nodeSpec.host
      }.toSet
      assert(!hosts.contains(firstHost))
    } finally
      client.close()
  }

  test("NodesClient.withFailover tries next candidate on failover-worthy failure") {
    val nodesClient = NodesClient(shard1)
    try {
      var attempts = 0
      val result = nodesClient.withFailover { c =>
        attempts += 1
        if (attempts == 1) {
          throw com.clickhouse.spark.exception.CHClientException("simulated down", Some(c.nodeSpec))
        }
        c.nodeSpec.host
      }
      assert(attempts === 2)
      assert(Set(n1.host, n2.host).contains(result))
      assert(NodeHealthTracker.deadKeys.nonEmpty)
    } finally
      nodesClient.close()
  }
}
