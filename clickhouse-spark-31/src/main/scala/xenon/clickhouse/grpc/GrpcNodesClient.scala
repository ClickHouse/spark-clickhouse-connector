package xenon.clickhouse.grpc

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Random._

import xenon.clickhouse.spec.{Nodes, NodeSpec}
import xenon.clickhouse.Logging

object GrpcNodesClient {
  def apply(nodes: Nodes) = new GrpcNodesClient(nodes)
}

class GrpcNodesClient(nodes: Nodes) extends AutoCloseable with Logging {
  assert(nodes.nodes.nonEmpty)

  @transient lazy val cache = new ConcurrentHashMap[NodeSpec, GrpcNodeClient]

  def node: GrpcNodeClient = {

    val nodeSpec = shuffle(nodes.nodes.toSeq).head
    cache.computeIfAbsent(
      nodeSpec,
      { nodeSpec =>
        log.info(s"Create gRPC client to ${nodeSpec.host}:${nodeSpec.grpc_port.get}")
        new GrpcNodeClient(nodeSpec)
      }
    )
  }

  override def close(): Unit = cache.asScala.values.foreach(_.close())
}
