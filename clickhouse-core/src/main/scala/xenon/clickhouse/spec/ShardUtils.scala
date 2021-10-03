package xenon.clickhouse.spec

object ShardUtils {

  def calcShard(cluster: ClusterSpec, value: Long): ShardSpec = {
    val shards = cluster.shards.sorted
    val weights = shards.map(_.weight)
    val lowerBounds = weights.indices.map(i => weights.slice(0, i).sum)
    val upperBounds = weights.indices.map(i => weights.slice(0, i + 1).sum)
    val ranges = (lowerBounds zip upperBounds).map { case (l, u) => l until u }
    val rem = value % weights.sum
    (shards zip ranges).find(_._2 contains rem).map(_._1).get
  }
}
