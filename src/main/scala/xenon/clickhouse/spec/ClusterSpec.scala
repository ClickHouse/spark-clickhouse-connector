package xenon.clickhouse.spec

import xenon.clickhouse.Utils._

case class NodeSpec(
  private val _host: String,
  private val _http_port: Option[Int] = None,
  private val _tcp_port: Option[Int] = None,
  private val _grpc_port: Option[Int] = None,
  username: String = "default",
  password: String = "",
  database: String = "default"
) {
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
}

case class ReplicaSpec(
  num: Int,
  node: NodeSpec
) extends Ordered[ReplicaSpec] {
  override def compare(that: ReplicaSpec): Int = Ordering[Int].compare(num, that.num)
}

case class ShardSpec(
  num: Int,
  weight: Int,
  replicas: Seq[ReplicaSpec]
) extends Ordered[ShardSpec] {
  override def compare(that: ShardSpec): Int = Ordering[Int].compare(num, that.num)
}

case class ClusterSpec(
  name: String,
  shards: Seq[ShardSpec]
) {
  def nodes: Seq[NodeSpec] = shards.flatten(_.replicas.map(_.node))
}
