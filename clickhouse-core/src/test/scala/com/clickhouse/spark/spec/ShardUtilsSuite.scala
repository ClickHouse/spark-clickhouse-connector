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

package com.clickhouse.spark.spec

import org.scalatest.funsuite.AnyFunSuite

class ShardUtilsSuite extends AnyFunSuite with NodeSpecHelper {

  test("test calculate shard") {
    assert(ShardUtils.calcShard(cluster, 0).num === 1)
    assert(ShardUtils.calcShard(cluster, 1).num === 2)
    assert(ShardUtils.calcShard(cluster, 2).num === 2)
    assert(ShardUtils.calcShard(cluster, 3).num === 1)
    assert(ShardUtils.calcShard(cluster, 4).num === 2)
    assert(ShardUtils.calcShard(cluster, 5).num === 2)
    assert(ShardUtils.calcShard(cluster, 6).num === 1)
    assert(ShardUtils.calcShard(cluster, 7).num === 2)
    assert(ShardUtils.calcShard(cluster, 8).num === 2)
  }

  test("test calculate shard treats hash value as unsigned") {
    // total weight is 3; -1 as unsigned is 2^64 - 1 ≡ 0 (mod 3), -2 is 2^64 - 2 ≡ 2 (mod 3)
    assert(ShardUtils.calcShard(cluster, -1).num === 1)
    assert(ShardUtils.calcShard(cluster, -2).num === 2)
  }

  test("test calculate shard never selects zero-weight shards") {
    val weighted = ClusterSpec(
      name = "cluster-zero-weight",
      shards = Array(
        ShardSpec(num = 1, weight = 0, replicas = Array(replica_s1r1)),
        ShardSpec(num = 2, weight = 2, replicas = Array(replica_s2r1))
      )
    )
    (0 to 3).foreach { hash =>
      assert(ShardUtils.calcShard(weighted, hash.toLong).num === 2)
    }
  }
}
