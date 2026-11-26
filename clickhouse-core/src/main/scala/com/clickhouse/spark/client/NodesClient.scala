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
import com.clickhouse.spark.spec.{NodeSpec, Nodes}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.util.Random.shuffle
import scala.util.control.NonFatal

object NodesClient {
  def apply(nodes: Nodes, queryTimeoutMs: Long = NodeClient.DEFAULT_QUERY_TIMEOUT_MS): NodesClient =
    new NodesClient(nodes, queryTimeoutMs)
}

class NodesClient(nodes: Nodes, queryTimeoutMs: Long = NodeClient.DEFAULT_QUERY_TIMEOUT_MS)
    extends AutoCloseable with Logging {
  assert(nodes.nodes.nonEmpty)

  @transient lazy val cache = new ConcurrentHashMap[NodeSpec, NodeClient]

  @transient @volatile private var sticky: Option[NodeSpec] = None

  def node: NodeClient =
    sticky match {
      case Some(spec) if !NodeHealthTracker.isDead(spec) =>
        cache.computeIfAbsent(
          spec,
          { nodeSpec =>
            log.info(s"Create client of $nodeSpec")
            new NodeClient(nodeSpec, queryTimeoutMs)
          }
        )
      case _ =>
        val pick = pickLive()
        sticky = Some(pick)
        cache.computeIfAbsent(
          pick,
          { nodeSpec =>
            log.info(s"Create client of $nodeSpec")
            new NodeClient(nodeSpec, queryTimeoutMs)
          }
        )
    }

  /**
   * Run `action` against the current node; on a failover-worthy failure mark it dead, pick another
   * live candidate, and retry until candidates are exhausted.
   */
  def withFailover[R](action: NodeClient => R): R = {
    var lastError: Throwable = null
    val tried = scala.collection.mutable.Set.empty[String]
    while (true) {
      val candidates = NodeHealthTracker.live(nodes.nodes.toSeq)
        .filterNot(n => tried.contains(NodeHealthTracker.key(n)))
      val pool = if (candidates.nonEmpty) candidates
      else nodes.nodes.toSeq.filterNot(n => tried.contains(NodeHealthTracker.key(n)))
      if (pool.isEmpty) {
        if (lastError != null) throw lastError
        throw CHClientException(
          s"No live ClickHouse replicas among ${nodes.nodes.map(_.toString).mkString("[", ", ", "]")}"
        )
      }
      val pick = shuffle(pool).head
      sticky = Some(pick)
      tried += NodeHealthTracker.key(pick)
      val client = cache.computeIfAbsent(
        pick,
        { nodeSpec =>
          log.info(s"Create client of $nodeSpec")
          new NodeClient(nodeSpec, queryTimeoutMs)
        }
      )
      try
        return action(client)
      catch {
        case NonFatal(t) if NodeFailover.isFailoverWorthy(t) =>
          log.warn(s"Failover from $pick after: ${t.getMessage}")
          evict(pick)
          lastError = t
        case NonFatal(t) =>
          throw t
      }
    }
    throw new IllegalStateException("unreachable")
  }

  def evict(nodeSpec: NodeSpec): Unit = {
    NodeHealthTracker.markDead(nodeSpec)
    if (sticky.exists(s => NodeHealthTracker.key(s) == NodeHealthTracker.key(nodeSpec))) {
      sticky = None
    }
    // ConcurrentHashMap keys are by reference; remove any matching key
    cache.asScala.keys.filter(k => NodeHealthTracker.key(k) == NodeHealthTracker.key(nodeSpec)).foreach { k =>
      Option(cache.remove(k)).foreach { c =>
        try c.close()
        catch { case NonFatal(_) => () }
      }
    }
  }

  private def pickLive(): NodeSpec = {
    val live = shuffle(NodeHealthTracker.live(nodes.nodes.toSeq))
    if (live.nonEmpty) live.head
    else shuffle(nodes.nodes.toSeq).head
  }

  override def close(): Unit = cache.asScala.values.foreach(_.close())
}
