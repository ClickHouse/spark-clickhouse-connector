package xenon.clickhouse

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Random._

import org.apache.spark.sql.clickhouse.ClickHouseAnalysisException
import xenon.clickhouse.spec.ClusterSpec

object GrpcClusterClient {
  def apply(cluster: ClusterSpec) = new GrpcClusterClient(cluster)
}

class GrpcClusterClient(cluster: ClusterSpec) extends AutoCloseable with Logging {

  @transient lazy val cache = new ConcurrentHashMap[(Int, Int), GrpcNodeClient]

  def node(shard: Option[Int] = None, replica: Option[Int] = None): GrpcNodeClient = {
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
      case _ =>
        throw ClickHouseAnalysisException(s"Invalid shard[$shard] replica[$replica] of cluster ${cluster.name}")
    }

    cache.computeIfAbsent(
      (_shard, _replica),
      { case (s, r) =>
        val shardSpec = cluster.shards.find(_.num == s).get
        val replicaSpec = shardSpec.replicas.find(_.num == r).get
        val nodeSpec = replicaSpec.node
        log.info(s"Create gRPC client to ${nodeSpec.host}:${nodeSpec.grpc_port}, shard $s replica $r")
        new GrpcNodeClient(nodeSpec)
      }
    )
  }

  override def close(): Unit = cache.asScala.values.foreach(_.close())
}
