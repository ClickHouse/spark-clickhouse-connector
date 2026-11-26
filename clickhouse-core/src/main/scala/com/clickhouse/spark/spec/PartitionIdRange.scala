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

/**
 * The half-open range `[lower, upper)` of `_partition_id` values one scan task reads; `None` leaves that side
 * unbounded, so a range unbounded on both sides is the whole table.
 *
 * Ids compare with `String` ordering. ClickHouse compares `String` bytewise, which agrees for ASCII, and partition
 * ids are ASCII (decimal numbers, dates such as `202105`, hex hashes, `all`).
 */
case class PartitionIdRange(lower: Option[String], upper: Option[String]) {

  def contains(partitionId: String): Boolean = lower.forall(partitionId >= _) && upper.forall(partitionId < _)

  /** The `_partition_id` condition of this range, or `None` when it is unbounded on both sides. */
  def filterExpr: Option[String] = {
    val conds = lower.map(id => s"_partition_id >= ${PartitionIdRange.quote(id)}").toSeq ++
      upper.map(id => s"_partition_id < ${PartitionIdRange.quote(id)}")
    if (conds.isEmpty) None else Some(conds.mkString(" AND "))
  }
}

object PartitionIdRange {

  /**
   * Tiles the whole `_partition_id` space with one range per distinct id: sorted, the i-th range starts at the i-th
   * id and ends at the next one, except that the first is unbounded below and the last unbounded above. Every
   * possible id - listed or not, such as a partition created after the listing - is in exactly one range, and the
   * i-th range contains the i-th id. No ids give one range covering everything. Linear after the sort.
   */
  def cover(ids: Seq[String]): Seq[PartitionIdRange] = {
    val sorted = ids.distinct.sorted.toVector
    if (sorted.isEmpty) Vector(PartitionIdRange(None, None))
    else {
      val inner = sorted.tail.map(Some(_))
      (None +: inner).zip(inner :+ None).map { case (lower, upper) => PartitionIdRange(lower, upper) }
    }
  }

  /** A `_partition_id` as a ClickHouse string literal. */
  def quote(partitionId: String): String = s"'${partitionId.replace("'", "''")}'"
}
