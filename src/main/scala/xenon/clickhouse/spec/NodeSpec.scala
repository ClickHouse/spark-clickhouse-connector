package xenon.clickhouse.spec

import xenon.clickhouse.Utils._

trait Nodes {
  def nodes: Array[NodeSpec]
}

case class NodeSpec(
  private val _host: String,
  private val _http_port: Option[Int] = None,
  private val _tcp_port: Option[Int] = None,
  private val _grpc_port: Option[Int] = None,
  username: String = "default",
  password: String = "",
  database: String = "default"
) extends Nodes {
  def host: String = findHost(_host)
  def http_port: Option[Int] = findPort(_http_port)
  def tcp_port: Option[Int] = findPort(_tcp_port)
  def grpc_port: Option[Int] = findPort(_grpc_port)

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

  override def nodes: Array[NodeSpec] = Array(this)
}

case class ReplicaSpec(
  num: Int,
  node: NodeSpec
) extends Ordered[ReplicaSpec] with Nodes {
  override def compare(that: ReplicaSpec): Int = Ordering[Int].compare(num, that.num)

  override def nodes: Array[NodeSpec] = Array(node)
}

case class ShardSpec(
  num: Int,
  weight: Int,
  replicas: Array[ReplicaSpec]
) extends Ordered[ShardSpec] with Nodes {
  override def compare(that: ShardSpec): Int = Ordering[Int].compare(num, that.num)

  override def nodes: Array[NodeSpec] = replicas.map(_.node)
}

case class ClusterSpec(
  name: String,
  shards: Array[ShardSpec]
) extends Nodes {
  override def nodes: Array[NodeSpec] = shards.toSeq.flatten(_.replicas.map(_.node)).toArray
}
