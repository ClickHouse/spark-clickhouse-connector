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
