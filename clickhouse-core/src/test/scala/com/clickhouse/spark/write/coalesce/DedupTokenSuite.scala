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

package com.clickhouse.spark.write.coalesce

import org.scalatest.funsuite.AnyFunSuite

class DedupTokenSuite extends AnyFunSuite {

  private def handle(id: String): BatchHandle =
    BatchHandle(ShardKey("q", "db", "t", None), id, 1L, new BytesPayload(Array[Byte](0)))

  test("token is stable for the same ordered group") {
    val g = Seq(handle("a"), handle("b"), handle("c"))
    assert(DedupToken.forGroup(g) == DedupToken.forGroup(g))
  }

  test("token differs across groups with different ids or order") {
    val g1 = Seq(handle("a"), handle("b"))
    val g2 = Seq(handle("a"), handle("c"))
    val g3 = Seq(handle("b"), handle("a"))
    assert(DedupToken.forGroup(g1) != DedupToken.forGroup(g2))
    assert(DedupToken.forGroup(g1) != DedupToken.forGroup(g3))
  }

  test("token is a non-empty hex string") {
    val t = DedupToken.forGroup(Seq(handle("a")))
    assert(t.nonEmpty && t.matches("[0-9a-f]+"))
  }
}
