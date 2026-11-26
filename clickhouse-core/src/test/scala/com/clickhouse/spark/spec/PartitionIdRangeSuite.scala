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

class PartitionIdRangeSuite extends AnyFunSuite {

  test("cover dedupes and sorts the ids with String order") {
    // "10" sorts before "3"
    assert(PartitionIdRange.cover(Seq("3", "1", "10", "1")) === Seq(
      PartitionIdRange(None, Some("10")),
      PartitionIdRange(Some("10"), Some("3")),
      PartitionIdRange(Some("3"), None)
    ))
  }

  test("cover is open at both ends and neighbouring ranges share a bound") {
    val ranges = PartitionIdRange.cover(Seq("a", "b", "c", "d"))
    assert(ranges.head.lower.isEmpty)
    assert(ranges.last.upper.isEmpty)
    ranges.zip(ranges.tail).foreach { case (prev, next) =>
      assert(prev.upper.isDefined)
      assert(prev.upper === next.lower)
    }
  }

  test("the i-th range contains the i-th sorted id") {
    val ids = Seq("40", "20", "a1", "6d2c8d1f0f4f8e0e9b1b4c5d6e7f8a9b")
    val ranges = PartitionIdRange.cover(ids)
    ids.sorted.zip(ranges).foreach { case (id, range) => assert(range.contains(id), id) }
  }

  test("cover puts every possible id in exactly one range") {
    val listed = Seq("20", "40", "6d2c8d1f0f4f8e0e9b1b4c5d6e7f8a9b", "a1")
    val ranges = PartitionIdRange.cover(listed)
    // listed ids, and ids below / between / above them such as partitions created after the listing
    (Seq("", "10", "30", "-5", "all", "zz") ++ listed).foreach { id =>
      assert(ranges.count(_.contains(id)) === 1, s"partition id '$id'")
    }
  }

  test("a single id, or none, gives one range covering the whole table") {
    Seq(Seq("202105"), Seq.empty[String]).foreach { ids =>
      val ranges = PartitionIdRange.cover(ids)
      assert(ranges === Seq(PartitionIdRange(None, None)), ids)
      assert(ranges.head.filterExpr.isEmpty)
    }
  }

  test("filterExpr renders the bounds that are set") {
    assert(PartitionIdRange(None, Some("b")).filterExpr === Some("_partition_id < 'b'"))
    assert(PartitionIdRange(Some("a"), Some("b")).filterExpr === Some("_partition_id >= 'a' AND _partition_id < 'b'"))
    assert(PartitionIdRange(Some("a"), None).filterExpr === Some("_partition_id >= 'a'"))
    assert(PartitionIdRange(None, None).filterExpr === None)
  }

  test("filterExpr escapes quotes in the bounds") {
    assert(PartitionIdRange.cover(Seq("a", "a'b")).map(_.filterExpr) ===
      Seq(Some("_partition_id < 'a''b'"), Some("_partition_id >= 'a''b'")))
  }

  test("cover is linear in the number of ids") {
    // a quadratic walk over a List would take billions of steps here
    val ids = (0 until 200000).map(i => f"$i%08d")
    val ranges = PartitionIdRange.cover(ids)
    assert(ranges.length === ids.length)
    assert(ranges(100000) === PartitionIdRange(Some("00100000"), Some("00100001")))
  }
}
