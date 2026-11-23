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

package com.clickhouse.spark.read.split.primaryindex

/**
 * How the primary index split sizes the read of a table: a task per `targetRowsPerTask` rows read, `atLeast` tasks at
 * the least and `maxParallelism`, when set, at the most.
 */
case class PrimaryIndexSplitSettings(atLeast: Int, targetRowsPerTask: Long, maxParallelism: Option[Int])

object PrimaryIndexSplitSettings {

  /** For one of `tables` sharing a read equally: its share of three tasks per core and of the maximum set. */
  def apply(
    maxParallelism: Option[Int],
    targetRowsPerTask: Long,
    defaultParallelism: Int,
    tables: Int
  ): PrimaryIndexSplitSettings = {
    def share(tasks: Int): Int = math.max(1, tasks / tables)
    PrimaryIndexSplitSettings(share(3 * defaultParallelism), targetRowsPerTask, maxParallelism.map(share))
  }
}
