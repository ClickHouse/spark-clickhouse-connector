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
  PartitionSpec,
  PrimaryKeyRange,
  TableEngineSpec,
  TableKeyRanges,
  TablePartition
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
  private val why = NotSplittable("the table has no primary key")
  private val split = TableKeyRanges(tableSpec(), NodeSpec("127.0.0.1"), Right(ranges))
  private val unsplit = TableKeyRanges(tableSpec(), NodeSpec("127.0.0.2"), Left(why))
  private val twoPartitions = Seq("1", "2")
    .map(id => TablePartition(tableSpec(), NodeSpec("127.0.0.1"), PartitionSpec(id, id, 1, 1)))

  private def resolve(
    strategy: String = TASK_SPLIT_STRATEGY_PRIMARY_INDEX,
    globalAggregatePushed: Boolean = false,
    primaryIndexSplitMaxParallelism: Option[Int] = None,
    engine: TableEngineSpec = mergeTree,
    convertLocal: Boolean = false,
    partitions: => Seq[TablePartition] = fail("the partitions must not be listed"),
    primaryKeyRanges: => Seq[TableKeyRanges] = fail("the primary index must not be planned")
  ): TaskSplit =
    TaskSplit.resolve(
      strategy,
      globalAggregatePushed,
      primaryIndexSplitMaxParallelism,
      engine,
      convertLocal,
      partitions,
      primaryKeyRanges
    )

  test("primaryIndexSplit splits by the planned ranges, or is one task for the planner's reason") {
    assert(resolve(primaryKeyRanges = Seq(split)) === PrimaryIndexSplit(Seq(split)))
    assert(resolve(primaryKeyRanges = Seq(unsplit)) === OneTask(NoKeyRanges(why)))
  }

  test("primaryIndexSplit with convertLocal splits a Distributed table shard by shard, an unsplit shard one task") {
    def byShard(shards: TableKeyRanges*): TaskSplit =
      resolve(engine = distributed, convertLocal = true, primaryKeyRanges = shards)
    assert(byShard(split, split) === PrimaryIndexSplit(Seq(split, split)))
    assert(byShard(split, unsplit) === PrimaryIndexSplit(Seq(split, unsplit)))
    assert(byShard(unsplit, unsplit) === PrimaryIndexSplit(Seq(unsplit, unsplit)))
    // a lone shard that can't be split is read through the Distributed engine
    assert(byShard(unsplit) === OneTask(NoKeyRanges(why)))
  }

  test("partitionSplit splits by the listed partitions, or is one task when at most one is listed") {
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, partitions = twoPartitions)
      === PartitionSplit(twoPartitions))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, partitions = twoPartitions.take(1))
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

  test("maxParallelism 1 is one task under primaryIndexSplit only, after oneTask and a global aggregate") {
    val one = Some(1)
    assert(resolve(primaryIndexSplitMaxParallelism = one) === OneTask(MaxParallelismOne))
    assert(resolve(engine = distributed, primaryIndexSplitMaxParallelism = one) === OneTask(MaxParallelismOne))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK, primaryIndexSplitMaxParallelism = one) === OneTask(Chosen))
    assert(resolve(globalAggregatePushed = true, primaryIndexSplitMaxParallelism = one) === OneTask(GlobalAggregate))
    // partitionSplit does not look at it
    assert(resolve(TASK_SPLIT_STRATEGY_PARTITION, primaryIndexSplitMaxParallelism = one, partitions = twoPartitions)
      === PartitionSplit(twoPartitions))
    // more than 1, the rows read decide
    assert(resolve(primaryIndexSplitMaxParallelism = Some(4), primaryKeyRanges = Seq(split))
      === PrimaryIndexSplit(Seq(split)))
  }

  test("a Distributed table is one task without convertLocal, which splits it by shard") {
    assert(resolve(engine = distributed) === OneTask(DistributedIndex))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_PARTITION, engine = distributed) === OneTask(DistributedPartitions))
    assert(resolve(
      strategy = TASK_SPLIT_STRATEGY_PARTITION,
      engine = distributed,
      convertLocal = true,
      partitions = twoPartitions
    ) === PartitionSplit(twoPartitions))
    assert(resolve(strategy = TASK_SPLIT_STRATEGY_ONE_TASK, engine = distributed, convertLocal = true)
      === OneTask(Chosen))
  }
}
