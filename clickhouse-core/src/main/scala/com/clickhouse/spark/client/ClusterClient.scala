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
import com.clickhouse.spark.spec.ClusterSpec

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.util.Random._

object ClusterClient {
  def apply(cluster: ClusterSpec) = new ClusterClient(cluster)
}

class ClusterClient(cluster: ClusterSpec) extends AutoCloseable with Logging {

  @transient lazy val cache = new ConcurrentHashMap[(Int, Int), NodeClient]

  def node(shard: Option[Int] = None, replica: Option[Int] = None): NodeClient = {
    val (_shard, _replica) = (shard, replica) match {
      case (Some(s), Some(r)) => (s, r)
      case (Some(s), None) =>
        val shardSpec = cluster.shards.filter(_.num == s).head
        val replicaSpec = shuffle(shardSpec.replicas.toSeq).head
        (s, replicaSpec.num)
      case (None, None) =>
        val shardSpec = shuffle(cluster.shards.toSeq).head
        val replicaSpec = shuffle(shardSpec.replicas.toSeq).head
        (shardSpec.num, replicaSpec.num)
      case _ => throw CHClientException(
          s"Invalid shard[${shard.orNull}] replica[${replica.orNull}] of cluster ${cluster.name}"
        )
    }

    cache.computeIfAbsent(
      (_shard, _replica),
      { case (s, r) =>
        val shardSpec = cluster.shards.find(_.num == s).get
        val replicaSpec = shardSpec.replicas.find(_.num == r).get
        val nodeSpec = replicaSpec.node
        log.info(s"Create client to $nodeSpec, shard $s replica $r")
        new NodeClient(nodeSpec)
      }
    )
  }

  override def close(): Unit = cache.asScala.values.foreach(_.close())
}
