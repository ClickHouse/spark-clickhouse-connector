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
import com.clickhouse.spark.spec.NodeSpec

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Process-local circuit breaker for ClickHouse nodes. Driver and each executor keep their own
 * instance; there is no cross-JVM broadcast. Keyed by host + HTTP/TCP ports so two NodeSpec values
 * that resolve to the same endpoint share a dead mark.
 */
object NodeHealthTracker extends Logging {

  private val dead = new ConcurrentHashMap[String, java.lang.Long]()

  def key(node: NodeSpec): String =
    s"${node.host}|${node.http_port.getOrElse(-1)}|${node.tcp_port.getOrElse(-1)}"

  def markDead(node: NodeSpec): Unit = {
    val k = key(node)
    if (dead.put(k, System.currentTimeMillis()) == null) {
      log.warn(s"Marking ClickHouse node dead for this JVM: $node")
    }
  }

  def isDead(node: NodeSpec): Boolean = dead.containsKey(key(node))

  def live(nodes: Seq[NodeSpec]): Seq[NodeSpec] = nodes.filterNot(isDead)

  def live(nodes: Array[NodeSpec]): Array[NodeSpec] = live(nodes.toSeq).toArray

  def clear(): Unit = dead.clear()

  def deadKeys: Set[String] = dead.keySet().asScala.toSet
}
