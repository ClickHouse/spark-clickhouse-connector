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
import com.clickhouse.spark.spec.{NodeSpec, Nodes}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.util.Random.shuffle

object NodesClient {
  def apply(nodes: Nodes) = new NodesClient(nodes)
}

class NodesClient(nodes: Nodes) extends AutoCloseable with Logging {
  assert(nodes.nodes.nonEmpty)

  @transient lazy val cache = new ConcurrentHashMap[NodeSpec, NodeClient]

  def node: NodeClient = {

    val nodeSpec = shuffle(nodes.nodes.toSeq).head
    cache.computeIfAbsent(
      nodeSpec,
      { nodeSpec =>
        log.info(s"Create client of $nodeSpec")
        new NodeClient(nodeSpec)
      }
    )
  }

  override def close(): Unit = cache.asScala.values.foreach(_.close())
}
