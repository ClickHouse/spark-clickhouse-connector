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

import org.scalatest.funsuite.AnyFunSuite

class NodeSpecSuite extends AnyFunSuite with NodeSpecHelper {

  test("nodes should be sorted") {
    assert(shard_s1.nodes.map(_.host) === Array("s1r1", "s1r2"))
    assert(shard_s2.nodes.map(_.host) === Array("s2r1", "s2r2"))

    assert(cluster.nodes.map(_.host) === Array("s1r1", "s1r2", "s2r1", "s2r2"))
    assert(cluster.nodes.map(_.host) === Array("s1r1", "s1r2", "s2r1", "s2r2"))
  }
}
