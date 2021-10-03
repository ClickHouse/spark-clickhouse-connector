/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.spec

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import xenon.clickhouse.ToJson
import xenon.clickhouse.Utils._

trait Nodes {
  def nodes: Array[NodeSpec]
}

case class NodeSpec(
  @JsonIgnore private val _host: String,
  @JsonIgnore private val _http_port: Option[Int] = None,
  @JsonIgnore private val _tcp_port: Option[Int] = None,
  @JsonIgnore private val _grpc_port: Option[Int] = None,
  @JsonProperty("username") username: String = "default",
  @JsonProperty("password") password: String = "",
  @JsonProperty("database") database: String = "default"
) extends Nodes with ToJson {
  @JsonProperty("host") def host: String = findHost(_host)
  @JsonProperty("http_port") def http_port: Option[Int] = findPort(_http_port)
  @JsonProperty("tcp_port") def tcp_port: Option[Int] = findPort(_tcp_port)
  @JsonProperty("grpc_port") def grpc_port: Option[Int] = findPort(_grpc_port)

  private def findHost(source: String): String =
    if (isTesting) {
      // workaround for testcontainers docker compose network mechanism
      sys.props.get(s"${PREFIX}_HOST_$source").getOrElse(source)
    } else source

  private def findPort(source: Option[Int]): Option[Int] =
    if (isTesting) {
      // workaround for testcontainers docker compose network mechanism
      source.map(p => sys.props.get(s"${PREFIX}_HOST_${_host}_PORT_$p").map(_.toInt).getOrElse(p))
    } else source

  @JsonIgnore override val nodes: Array[NodeSpec] = Array(this)
}

case class ReplicaSpec(
  num: Int,
  node: NodeSpec
) extends Ordered[ReplicaSpec] with Nodes with ToJson {

  override def compare(that: ReplicaSpec): Int = Ordering[Int].compare(num, that.num)

  @JsonIgnore override val nodes: Array[NodeSpec] = Array(node)
}

case class ShardSpec(
  num: Int,
  weight: Int,
  replicas: Array[ReplicaSpec]
) extends Ordered[ShardSpec] with Nodes with ToJson {

  override def compare(that: ShardSpec): Int = Ordering[Int].compare(num, that.num)

  @JsonIgnore override lazy val nodes: Array[NodeSpec] = replicas.sorted.flatMap(_.nodes)
}

case class ClusterSpec(
  name: String,
  shards: Array[ShardSpec]
) extends Nodes with ToJson {

  @JsonIgnore override lazy val nodes: Array[NodeSpec] = shards.sorted.flatMap(_.nodes)
}
