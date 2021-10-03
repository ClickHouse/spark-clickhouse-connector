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
