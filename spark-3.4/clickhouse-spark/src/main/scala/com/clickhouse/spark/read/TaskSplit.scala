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

import com.clickhouse.spark.spec.{DistributedEngineSpec, NotSplittable, PartitionGroup, TableEngineSpec, TableKeyRanges}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._

/** How a read is split into tasks. Every task's table, nodes and filter follow from it alone. */
sealed trait TaskSplit

object TaskSplit {

  /** The whole table in one task; a Distributed table through its engine. */
  case class OneTask(reason: OneTaskReason) extends TaskSplit

  /** One task per group of ClickHouse partitions; of every shard's local table for a Distributed table. */
  case class PartitionSplit(groups: Seq[PartitionGroup]) extends TaskSplit

  /**
   * One task per range of the primary key; of every shard's local table for a Distributed table, a shard that can't
   * be split being one task.
   */
  case class PrimaryIndexSplit(tables: Seq[TableKeyRanges]) extends TaskSplit

  /** Why a read is one task. */
  sealed trait OneTaskReason {
    def message: String
  }

  object OneTaskReason {

    /** One task is what the read asks for or needs. */
    sealed abstract class Intended(val message: String) extends OneTaskReason

    /** The configured split does not apply to the read, which may impact read performance. */
    sealed abstract class Fallback(val message: String) extends OneTaskReason

    case object Chosen extends Intended(s"${READ_TASK_SPLIT_STRATEGY.key} is $TASK_SPLIT_STRATEGY_ONE_TASK")
    case object GlobalAggregate extends Intended("an aggregation is pushed down without GROUP BY")
    case object MaxParallelismOne extends Intended(s"${READ_MAX_PARALLELISM.key} is 1")
    case object DistributedPartitions extends Fallback(
          "the partitions of a Distributed table can't be listed through its engine; " +
            s"${READ_DISTRIBUTED_CONVERT_LOCAL.key} reads it by shard and partition"
        )
    case object DistributedIndex extends Fallback(
          "a Distributed table has no primary index across shards; " +
            s"${READ_DISTRIBUTED_CONVERT_LOCAL.key} reads it by shard and key range"
        )
    case object SinglePartition extends Fallback("system.parts lists at most one partition")
    case class NoKeyRanges(why: NotSplittable) extends Fallback(why.reason)
  }

  import OneTaskReason._

  /**
   * Decides the split for the configured `strategy`. Each strategy falls back to one task on its own, never to
   * another split, and says why. `partitionGroups` and `primaryKeyRanges` query ClickHouse and come sized for
   * `maxParallelism`, so they are by-name and only evaluated when their strategy gets that far. `primaryKeyRanges`
   * holds the table read, or every shard's local table for a Distributed table.
   */
  def resolve(
    strategy: String,
    globalAggregatePushed: Boolean,
    maxParallelism: Int,
    engine: TableEngineSpec,
    convertDistributedToLocal: Boolean,
    partitionGroups: => Seq[PartitionGroup],
    primaryKeyRanges: => Seq[TableKeyRanges]
  ): TaskSplit = {
    val distributed = engine.isInstanceOf[DistributedEngineSpec]
    strategy match {
      case TASK_SPLIT_STRATEGY_ONE_TASK => OneTask(Chosen)
      case _ if globalAggregatePushed => OneTask(GlobalAggregate)
      case _ if maxParallelism == 1 => OneTask(MaxParallelismOne)
      case TASK_SPLIT_STRATEGY_PARTITION if distributed && !convertDistributedToLocal => OneTask(DistributedPartitions)
      case TASK_SPLIT_STRATEGY_PARTITION =>
        val groups = partitionGroups
        if (groups.size <= 1) OneTask(SinglePartition) else PartitionSplit(groups)
      case TASK_SPLIT_STRATEGY_PRIMARY_INDEX if distributed && !convertDistributedToLocal => OneTask(DistributedIndex)
      case TASK_SPLIT_STRATEGY_PRIMARY_INDEX =>
        primaryKeyRanges match {
          case Seq(TableKeyRanges(_, _, Left(why))) => OneTask(NoKeyRanges(why))
          case tables => PrimaryIndexSplit(tables)
        }
    }
  }
}
