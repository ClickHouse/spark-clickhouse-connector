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

import com.clickhouse.spark.Logging
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.spec.{ClusterSpec, NodeSpec, ReplicaSpec, ShardSpec}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.util.Random.shuffle
import scala.util.control.NonFatal

object ClusterClient {
  def apply(cluster: ClusterSpec, queryTimeoutMs: Long = NodeClient.DEFAULT_QUERY_TIMEOUT_MS): ClusterClient =
    new ClusterClient(cluster, queryTimeoutMs)
}

class ClusterClient(cluster: ClusterSpec, queryTimeoutMs: Long = NodeClient.DEFAULT_QUERY_TIMEOUT_MS)
    extends AutoCloseable with Logging {

  @transient lazy val cache = new ConcurrentHashMap[(Int, Int), NodeClient]

  /** Last (shard, replica) returned by [[node]] for a given shard key (or -1 for any-shard). */
  @transient private lazy val lastPick = new ConcurrentHashMap[Int, (Int, Int)]()

  def node(shard: Option[Int] = None, replica: Option[Int] = None): NodeClient = {
    val (_shard, _replica) = (shard, replica) match {
      case (Some(s), Some(r)) => (s, r)
      case (Some(s), None) =>
        val shardSpec = findShard(s)
        val replicaSpec = pickLiveReplica(shardSpec)
        (s, replicaSpec.num)
      case (None, None) =>
        val shardSpec = shuffle(cluster.shards.toSeq).head
        val replicaSpec = pickLiveReplica(shardSpec)
        (shardSpec.num, replicaSpec.num)
      case _ => throw CHClientException(
          s"Invalid shard[${shard.orNull}] replica[${replica.orNull}] of cluster ${cluster.name}"
        )
    }

    val client = cache.computeIfAbsent(
      (_shard, _replica),
      { case (s, r) =>
        val shardSpec = findShard(s)
        val replicaSpec = shardSpec.replicas.find(_.num == r).get
        val nodeSpec = replicaSpec.node
        log.info(s"Create client to $nodeSpec, shard $s replica $r")
        new NodeClient(nodeSpec, queryTimeoutMs)
      }
    )
    lastPick.put(shard.getOrElse(-1), (_shard, _replica))
    client
  }

  /**
   * Mark the current sticky pick for `shard` dead, evict its cached client, and return a client to
   * another live replica of the same shard (or any shard when `shard` is empty).
   */
  def failover(shard: Option[Int]): NodeClient = {
    val key = shard.getOrElse(-1)
    Option(lastPick.get(key)).foreach { case (s, r) =>
      evict(s, r)
    }
    // Force a fresh pick that skips dead replicas
    node(shard, None)
  }

  def evict(shardNum: Int, replicaNum: Int): Unit =
    Option(cache.remove((shardNum, replicaNum))).foreach { client =>
      NodeHealthTracker.markDead(client.nodeSpec)
      try client.close()
      catch { case NonFatal(_) => () }
      log.info(s"Evicted client for shard $shardNum replica $replicaNum (${client.nodeSpec})")
    }

  def evict(nodeSpec: NodeSpec): Unit =
    cache.asScala.foreach {
      case ((s, r), client) if NodeHealthTracker.key(client.nodeSpec) == NodeHealthTracker.key(nodeSpec) =>
        evict(s, r)
      case _ => ()
    }

  // node clients opened so far, one per distinct (shard, replica); never evicted until failover
  def openClients: Int = cache.size()

  override def close(): Unit = cache.asScala.values.foreach(_.close())

  private def findShard(num: Int): ShardSpec =
    cluster.shards.find(_.num == num).getOrElse(
      throw CHClientException(s"Shard $num not found in cluster ${cluster.name}")
    )

  private def pickLiveReplica(shardSpec: ShardSpec): ReplicaSpec = {
    val live = shuffle(shardSpec.replicas.toSeq.filterNot(r => NodeHealthTracker.isDead(r.node)))
    if (live.nonEmpty) {
      live.head
    } else if (shardSpec.replicas.nonEmpty) {
      // All marked dead — fall back to any replica so a recovered node can be retried
      val any = shuffle(shardSpec.replicas.toSeq).head
      log.warn(
        s"All replicas of shard ${shardSpec.num} in cluster ${cluster.name} are marked dead; " +
          s"retrying ${any.node}"
      )
      any
    } else {
      throw CHClientException(s"Shard ${shardSpec.num} of cluster ${cluster.name} has no replicas")
    }
  }
}
