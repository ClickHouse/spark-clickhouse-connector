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

package com.clickhouse.spark.read

import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.read.TaskSplit._
import com.clickhouse.spark.spec.{DistributedEngineSpec, MergeTreeEngineSpec, PrimaryKeyRange, TableEngineSpec}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.scalatest.funsuite.AnyFunSuite

class TaskSplitSuite extends AnyFunSuite {

  private val mergeTree: TableEngineSpec = MergeTreeEngineSpec(engine_clause = "MergeTree")
  private val distributed: TableEngineSpec = DistributedEngineSpec(
    engine_clause = "Distributed('c', 'db', 'local')",
    cluster = "c",
    local_db = "db",
    local_table = "local"
  )
  private val ranges =
    Seq(PrimaryKeyRange(Seq("id"), None, Some(Seq("10"))), PrimaryKeyRange(Seq("id"), Some(Seq("10")), None))

  private def resolve(
    strategy: String = TASK_SPLIT_STRATEGY_PRIMARY_INDEX,
    globalAggregatePushed: Boolean = false,
    engine: TableEngineSpec = mergeTree,
    convertLocal: Boolean = false,
    useClusterNodes: Boolean = false,
    primaryKeyRanges: => Option[Seq[PrimaryKeyRange]] = fail("the primary index must not be planned")
  ): TaskSplit =
    TaskSplit.resolve(strategy, globalAggregatePushed, engine, convertLocal, useClusterNodes, primaryKeyRanges)

  test("primaryIndexSplit splits by the planned ranges and falls back to one task without them") {
    assert(resolve(primaryKeyRanges = Some(ranges)) === PrimaryIndexSplit(ranges))
    assert(resolve(primaryKeyRanges = None) === OneTask)
  }

  test("partitionSplit and oneTask never plan the primary index") {
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION) === PartitionSplit)
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK) === OneTask)
  }

  test("a global aggregate is one task under every strategy and engine") {
    Seq(TASK_SPLIT_STRATEGY_PRIMARY_INDEX, TASK_SPLIT_STRATEGY_PARTITION).foreach { strategy =>
      assert(resolve(strategy = strategy, globalAggregatePushed = true) === OneTask)
      assert(resolve(strategy = strategy, globalAggregatePushed = true, engine = distributed, convertLocal = true)
        === OneTask)
    }
  }

  test("a Distributed table is one task, except partitionSplit with convertLocal splits it by shard and partition") {
    assert(resolve(engine = distributed, convertLocal = true) === OneTask)
    assert(resolve(
      strategy = TASK_SPLIT_STRATEGY_PARTITION,
      engine = distributed,
      convertLocal = true
    ) === PartitionSplit)
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, engine = distributed) === OneTask)
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK, engine = distributed, convertLocal = true) === OneTask)
  }

  test("useClusterNodes without convertLocal is rejected for a Distributed table under every strategy") {
    Seq(TASK_SPLIT_STRATEGY_ONE_TASK, TASK_SPLIT_STRATEGY_PARTITION, TASK_SPLIT_STRATEGY_PRIMARY_INDEX).foreach { s =>
      intercept[CHClientException](resolve(strategy = s, engine = distributed, useClusterNodes = true))
    }
    assert(
      resolve(
        strategy = TASK_SPLIT_STRATEGY_PARTITION,
        engine = distributed,
        convertLocal = true,
        useClusterNodes = true
      )
        === PartitionSplit
    )
  }
}
