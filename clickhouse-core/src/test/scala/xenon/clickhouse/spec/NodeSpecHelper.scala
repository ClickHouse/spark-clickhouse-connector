package xenon.clickhouse.spec

trait NodeSpecHelper {

  val node_s1r1: NodeSpec = NodeSpec("s1r1")
  val node_s1r2: NodeSpec = NodeSpec("s1r2")
  val node_s2r1: NodeSpec = NodeSpec("s2r1")
  val node_s2r2: NodeSpec = NodeSpec("s2r2")

  val replica_s1r1: ReplicaSpec = ReplicaSpec(1, node_s1r1)
  val replica_s1r2: ReplicaSpec = ReplicaSpec(2, node_s1r2)
  val replica_s2r1: ReplicaSpec = ReplicaSpec(1, node_s2r1)
  val replica_s2r2: ReplicaSpec = ReplicaSpec(2, node_s2r2)

  val shard_s1: ShardSpec = ShardSpec(
    num = 1,
    weight = 1,
    replicas = Array(replica_s1r1, replica_s1r2) // sorted
  )

  val shard_s2: ShardSpec = ShardSpec(
    num = 2,
    weight = 2,
    replicas = Array(replica_s2r2, replica_s2r1) // unsorted
  )

  val cluster: ClusterSpec =
    ClusterSpec(
      name = "cluster-s2r2",
      shards = Array(shard_s2, shard_s1) // unsorted
    )
}
