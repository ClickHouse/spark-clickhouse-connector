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

import com.clickhouse.spark.spec.PartitionGroups._
import com.clickhouse.spark.spec.TableSpecs.tableSpec
import org.scalatest.funsuite.AnyFunSuite

class PartitionGroupsSuite extends AnyFunSuite {

  private def partition(id: String, rows: Long): PartitionSpec = PartitionSpec(id, id, rows, rows)

  private def filterExpr(partitions: Seq[PartitionSpec], partitionKey: String, byId: Boolean): String =
    PartitionGroup(tableSpec(partitionKey = partitionKey), NodeSpec("127.0.0.1"), partitions).filterExpr(byId)

  test("partitions are spread over at most the given tasks by rows, largest first") {
    val partitions = Seq(5L, 4L, 3L, 2L, 1L).map(rows => partition(rows.toString, rows))
    assert(group(partitions, 2).map(_.map(_.partition_id)) === Seq(Seq("5", "2", "1"), Seq("4", "3")))
    assert(group(partitions, 9).map(_.map(_.partition_id)) === Seq(Seq("5"), Seq("4"), Seq("3"), Seq("2"), Seq("1")))
    assert(group(partitions, 1).map(_.map(_.partition_id)) === Seq(Seq("5", "4", "3", "2", "1")))
    assert(group(Nil, 3).isEmpty)
    intercept[IllegalArgumentException](group(partitions, 0))
  }

  test("partitions without rows are spread by count") {
    val partitions = Seq("a", "b", "c").map(partition(_, 0))
    assert(group(partitions, 2).map(_.map(_.partition_id)) === Seq(Seq("a", "c"), Seq("b")))
  }

  test("a group is filtered by its partition ids or by the values of the partition key") {
    val months = Seq(PartitionSpec("202401", "202401", 1, 1), PartitionSpec("202402", "202402", 1, 1))
    assert(filterExpr(months, "toYYYYMM(d)", byId = true) === "_partition_id IN ('202401', '202402')")
    assert(filterExpr(months, "toYYYYMM(d)", byId = false) === "toYYYYMM(d) IN (202401, 202402)")
    val day = Seq(PartitionSpec("2024-01-01", "20240101", 1, 1))
    assert(filterExpr(day, "d", byId = false) === "d IN ('2024-01-01')")
    val tuple = Seq(PartitionSpec("(1,'2024-01-01')", "1-20240101", 1, 1))
    assert(filterExpr(tuple, "(m, d)", byId = false) === "(m, d) IN ((1,'2024-01-01'))")
  }

  test("no partition is the whole table") {
    assert(filterExpr(Seq(NoPartitionSpec), "d", byId = true) === "1=1")
    assert(filterExpr(Seq(NoPartitionSpec), "d", byId = false) === "1=1")
  }
}
