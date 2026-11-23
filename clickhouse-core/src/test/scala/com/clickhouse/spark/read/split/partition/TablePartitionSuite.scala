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

package com.clickhouse.spark.read.split.partition

import com.clickhouse.spark.spec.TableSpecs.tableSpec
import com.clickhouse.spark.spec.{NoPartitionSpec, NodeSpec, PartitionSpec}
import org.scalatest.funsuite.AnyFunSuite

class TablePartitionSuite extends AnyFunSuite {

  private def filterExpr(partition: PartitionSpec, partitionKey: String, byId: Boolean): String =
    TablePartition(tableSpec(partitionKey = partitionKey), NodeSpec("127.0.0.1"), partition).filterExpr(byId)

  test("a partition is filtered by its partition id or by the value of the partition key") {
    val month = PartitionSpec("202401", "202401", 1, 1)
    assert(filterExpr(month, "toYYYYMM(d)", byId = true) === "_partition_id = '202401'")
    assert(filterExpr(month, "toYYYYMM(d)", byId = false) === "toYYYYMM(d) = 202401")
    val day = PartitionSpec("2024-01-01", "20240101", 1, 1)
    assert(filterExpr(day, "d", byId = false) === "d = '2024-01-01'")
    val tuple = PartitionSpec("(1,'2024-01-01')", "1-20240101", 1, 1)
    assert(filterExpr(tuple, "(m, d)", byId = false) === "(m, d) = (1,'2024-01-01')")
  }

  test("no partition is the whole table") {
    assert(filterExpr(NoPartitionSpec, "d", byId = true) === "1=1")
    assert(filterExpr(NoPartitionSpec, "d", byId = false) === "1=1")
  }
}
