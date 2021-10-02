package xenon.clickhouse.spec

import org.scalatest.funsuite.AnyFunSuite

class ShardUtilsSuite extends AnyFunSuite with NodeSpecHelper {

  test("test calculate shard") {
    assert(ShardUtils.calcShard(cluster, 0).num === 1 )
    assert(ShardUtils.calcShard(cluster, 1).num === 2 )
    assert(ShardUtils.calcShard(cluster, 2).num === 2 )
    assert(ShardUtils.calcShard(cluster, 3).num === 1 )
    assert(ShardUtils.calcShard(cluster, 4).num === 2 )
    assert(ShardUtils.calcShard(cluster, 5).num === 2 )
    assert(ShardUtils.calcShard(cluster, 6).num === 1 )
    assert(ShardUtils.calcShard(cluster, 7).num === 2 )
    assert(ShardUtils.calcShard(cluster, 8).num === 2 )
  }
}
