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

import com.clickhouse.spark.spec.{DistributedEngineSpec, NotSplittable, PrimaryKeyRange, TableEngineSpec}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._

/** How a read is split into tasks. The task count and every task's filter follow from it alone. */
sealed trait TaskSplit

object TaskSplit {

  /** The whole table in one task; a Distributed table through its engine. */
  case class OneTask(reason: OneTaskReason) extends TaskSplit

  /** One task per group of ClickHouse partitions; of every shard's local table for a Distributed table. */
  case class PartitionSplit(partitions: Seq[ClickHouseInputPartition]) extends TaskSplit

  /** One task per range of the primary key. */
  case class PrimaryIndexSplit(ranges: Seq[PrimaryKeyRange]) extends TaskSplit

  /** Why a read is one task: asked for, or the configured split does not apply to it. */
  sealed abstract class OneTaskReason(val message: String)

  object OneTaskReason {
    case object Chosen extends OneTaskReason(s"${READ_TASK_SPLIT_STRATEGY.key} is $TASK_SPLIT_STRATEGY_ONE_TASK")
    case object GlobalAggregate extends OneTaskReason("an aggregation is pushed down without GROUP BY")
    case object MaxParallelismOne extends OneTaskReason(s"${READ_MAX_PARALLELISM.key} is 1")
    case object DistributedPartitions extends OneTaskReason(
          "the partitions of a Distributed table can't be listed through its engine; " +
            s"${READ_DISTRIBUTED_CONVERT_LOCAL.key} reads it by shard and partition"
        )
    case object DistributedIndex extends OneTaskReason(
          "a Distributed table has no primary index across shards; " +
            s"${READ_TASK_SPLIT_STRATEGY.key}=$TASK_SPLIT_STRATEGY_PARTITION with ${READ_DISTRIBUTED_CONVERT_LOCAL.key} " +
            "reads it by shard and partition"
        )
    case object SinglePartition extends OneTaskReason("system.parts lists at most one partition")
    case class NoKeyRanges(why: NotSplittable) extends OneTaskReason(why.reason)
  }

  import OneTaskReason._

  /**
   * Decides the split for the configured `strategy`. Each strategy falls back to one task on its own, never to
   * another split, and says why. `partitions` and `primaryKeyRanges` query ClickHouse and already respect
   * `maxParallelism`, so they are by-name and only evaluated when their strategy gets that far.
   */
  def resolve(
    strategy: String,
    globalAggregatePushed: Boolean,
    maxParallelism: Int,
    engine: TableEngineSpec,
    convertDistributedToLocal: Boolean,
    partitions: => Seq[ClickHouseInputPartition],
    primaryKeyRanges: => Either[NotSplittable, Seq[PrimaryKeyRange]]
  ): TaskSplit = {
    val distributed = engine.isInstanceOf[DistributedEngineSpec]
    strategy match {
      case TASK_SPLIT_STRATEGY_ONE_TASK => OneTask(Chosen)
      case _ if globalAggregatePushed => OneTask(GlobalAggregate)
      case _ if maxParallelism == 1 => OneTask(MaxParallelismOne)
      case TASK_SPLIT_STRATEGY_PARTITION if distributed && !convertDistributedToLocal => OneTask(DistributedPartitions)
      case TASK_SPLIT_STRATEGY_PARTITION =>
        val listed = partitions
        if (listed.size <= 1) OneTask(SinglePartition) else PartitionSplit(listed)
      case TASK_SPLIT_STRATEGY_PRIMARY_INDEX if distributed => OneTask(DistributedIndex)
      case TASK_SPLIT_STRATEGY_PRIMARY_INDEX =>
        primaryKeyRanges.fold(why => OneTask(NoKeyRanges(why)), ranges => PrimaryIndexSplit(ranges))
    }
  }
}
