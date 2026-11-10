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

import com.clickhouse.spark.read.TaskSplit._
import com.clickhouse.spark.read.TaskSplit.OneTaskReason._
import com.clickhouse.spark.spec.{
  DistributedEngineSpec,
  MergeTreeEngineSpec,
  NodeSpec,
  NotSplittable,
  PrimaryKeyRange,
  TableEngineSpec
}
import com.clickhouse.spark.spec.TableSpecs.tableSpec
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
  private val partitions = Seq("_partition_id = '1'", "_partition_id = '2'")
    .map(filterExpr => ClickHouseInputPartition(tableSpec(), filterExpr, NodeSpec("127.0.0.1")))

  private def resolve(
    strategy: String = TASK_SPLIT_STRATEGY_PRIMARY_INDEX,
    globalAggregatePushed: Boolean = false,
    maxParallelism: Int = 4,
    engine: TableEngineSpec = mergeTree,
    convertLocal: Boolean = false,
    partitions: => Seq[ClickHouseInputPartition] = fail("the partitions must not be listed"),
    primaryKeyRanges: => Either[NotSplittable, Seq[PrimaryKeyRange]] = fail("the primary index must not be planned")
  ): TaskSplit =
    TaskSplit.resolve(
      strategy,
      globalAggregatePushed,
      maxParallelism,
      engine,
      convertLocal,
      partitions,
      primaryKeyRanges
    )

  test("primaryIndexSplit splits by the planned ranges, or is one task for the planner's reason") {
    assert(resolve(primaryKeyRanges = Right(ranges)) === PrimaryIndexSplit(ranges))
    val why = NotSplittable("the table has no primary key")
    assert(resolve(primaryKeyRanges = Left(why)) === OneTask(NoKeyRanges(why)))
  }

  test("partitionSplit splits by the listed partitions, or is one task when at most one is listed") {
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, partitions = partitions) === PartitionSplit(partitions))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, partitions = partitions.take(1))
      === OneTask(SinglePartition))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, partitions = Nil) === OneTask(SinglePartition))
  }

  test("oneTask and a global aggregate neither list partitions nor plan the primary index") {
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK) === OneTask(Chosen))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK, globalAggregatePushed = true) === OneTask(Chosen))
    Seq(TASK_SPLIT_STRATEGY_PRIMARY_INDEX, TASK_SPLIT_STRATEGY_PARTITION).foreach { strategy =>
      assert(resolve(strategy = strategy, globalAggregatePushed = true) === OneTask(GlobalAggregate))
      assert(resolve(strategy = strategy, globalAggregatePushed = true, engine = distributed, convertLocal = true)
        === OneTask(GlobalAggregate))
    }
  }

  test("maxParallelism 1 is one task under both splits, after oneTask and a global aggregate") {
    assert(resolve(maxParallelism = 1) === OneTask(MaxParallelismOne))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, maxParallelism = 1) === OneTask(MaxParallelismOne))
    assert(resolve(engine = distributed, maxParallelism = 1) === OneTask(MaxParallelismOne))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK, maxParallelism = 1) === OneTask(Chosen))
    assert(resolve(globalAggregatePushed = true, maxParallelism = 1) === OneTask(GlobalAggregate))
  }

  test("a Distributed table is one task, except partitionSplit with convertLocal splits it by shard and partition") {
    assert(resolve(engine = distributed) === OneTask(DistributedIndex))
    assert(resolve(engine = distributed, convertLocal = true) === OneTask(DistributedIndex))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, engine = distributed) === OneTask(DistributedPartitions))
    assert(resolve(
      strategy = TASK_SPLIT_STRATEGY_PARTITION,
      engine = distributed,
      convertLocal = true,
      partitions = partitions
    ) === PartitionSplit(partitions))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK, engine = distributed, convertLocal = true)
      === OneTask(Chosen))
  }
}
