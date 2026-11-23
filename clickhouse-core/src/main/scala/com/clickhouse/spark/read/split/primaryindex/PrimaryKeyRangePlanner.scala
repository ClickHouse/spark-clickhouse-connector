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

import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.exception.ClickHouseErrCode
import com.clickhouse.spark.expr.PushedFilter
import com.clickhouse.spark.spec.TableSpec

import scala.util.control.NonFatal

/** Plans the primary key ranges a table is read in, from its primary index. */
object PrimaryKeyRangePlanner {

  // `mergeTreeIndex` returns these, then the primary key columns
  private val IndexColumns = Set("part_name", "mark_number", "rows_in_granule")

  // longer cut values are skipped, as each range repeats its bounds in every scan query
  private val MaxLiteralLength = 1024

  /**
   * Splits `table` into ranges of its primary key holding about the same number of rows, cut from the primary index
   * without reading data: two at least, as many as `settings` size it to. Or why the table can't be split by its
   * primary key.
   *
   * @param tableColumns
   *   every column with its ClickHouse type, telling key columns from key expressions
   * @param filters
   *   the pushed-down filters; those the index can evaluate limit the cuts to the rows read
   */
  def plan(
    table: TableSpec,
    tableColumns: Map[String, String],
    filters: Seq[PushedFilter],
    settings: PrimaryIndexSplitSettings
  )(implicit nodeClient: NodeClient): Either[NotSplittable, Seq[PrimaryKeyRange]] =
    try
      for {
        _ <- check(table.engine.endsWith("MergeTree"), s"the ${table.engine} engine has no primary index")
        _ <- check(table.primary_key.nonEmpty, "the table has no primary key")
        indexColumns <- describeIndex(table).toRight(NotSplittable("reading the primary index needs ClickHouse 24.2+"))
        primaryKey <- SqlText.splitArgs(table.primary_key).toRight(NotSplittable("the primary key can't be read"))
        _ <- check(
          indexColumns.count { case (name, _) => !IndexColumns.contains(name) } == primaryKey.size,
          "the primary index does not list the primary key"
        )
        key = splittableKeyPrefix(indexColumns, primaryKey, tableColumns)
        _ <- check(
          key.nonEmpty,
          "the primary key does not start with an integer, date, date time, string or UUID column or expression"
        )
        cuts = cutPoints(table, key, filters, settings)
        _ <- check(cuts.nonEmpty, "the primary index yields no cut point within the filters")
      } yield ranges(key.map(_.sql), cuts)
    catch {
      case NonFatal(e) => Left(NotSplittable("planning the primary key ranges failed", Some(e)))
    }

  // a step of the plan: passes when `holds`, else ends it with why the table can't be split
  private def check(holds: Boolean, whyNot: String): Either[NotSplittable, Unit] =
    if (holds) Right(()) else Left(NotSplittable(whyNot))

  // None before ClickHouse 24.2, which lacks `mergeTreeIndex`
  private def describeIndex(table: TableSpec)(implicit nodeClient: NodeClient): Option[Seq[(String, String)]] =
    nodeClient.syncQueryOutputJSONEachRow(
      s"DESCRIBE TABLE mergeTreeIndex(${SqlText.quote(table.database)}, ${SqlText.quote(table.name)})"
    ) match {
      case Left(e) if e.code == ClickHouseErrCode.UNKNOWN_FUNCTION.code() => None
      case Left(e) => throw e
      case Right(output) => Some(output.records.toList.map(row => row.get("name").asText -> row.get("type").asText))
    }

  /**
   * The longest prefix of the primary key that ranges can be cut on, columns and expressions alike. `primaryKey`
   * spells the entries as ClickHouse formats them, in the order of the index columns, which name them with their
   * identifiers unquoted.
   */
  private[primaryindex] def splittableKeyPrefix(
    indexColumns: Seq[(String, String)],
    primaryKey: Seq[String],
    tableColumns: Map[String, String]
  ): Seq[KeyEntry] =
    indexColumns
      .filterNot { case (name, _) => IndexColumns.contains(name) }
      .zip(primaryKey)
      .map { case ((name, chType), spelling) => KeyEntry.of(name, chType, spelling, tableColumns) }
      .takeWhile(_.isDefined)
      .flatten

  /** The keys to cut at, in key order. The smallest is dropped, as cutting there would leave the first range empty. */
  private def cutPoints(
    table: TableSpec,
    key: Seq[KeyEntry],
    filters: Seq[PushedFilter],
    settings: PrimaryIndexSplitSettings
  )(implicit nodeClient: NodeClient): Seq[Seq[String]] =
    nodeClient.syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(cutsQuery(table, key, filters, settings))
      .records.toList
      .map(row => key.zip(row).map { case (entry, value) => entry.keyType.literal(value.asText) })
      .distinct
      .drop(1)
      .filterNot(_.exists(_.length > MaxLiteralLength))

  /** The `[lower, upper)` ranges between consecutive cuts, open-ended at both ends. */
  private def ranges(key: Seq[String], cuts: Seq[Seq[String]]): Seq[PrimaryKeyRange] = {
    val bounds = cuts.map(Some(_))
    (None +: bounds).zip(bounds :+ None).map { case (lower, upper) => PrimaryKeyRange(key, lower, upper) }
  }

  /**
   * Orders the granules the filters keep by the key they start at and returns the key of the first granule in each
   * `1/slices` of their rows, so ranges cut there hold about as many rows each and none less than a granule.
   * `slices` is one per `targetRowsPerTask` of those rows, `atLeast` at the least and `maxParallelism` at the most.
   */
  private[primaryindex] def cutsQuery(
    table: TableSpec,
    key: Seq[KeyEntry],
    filters: Seq[PushedFilter],
    settings: PrimaryIndexSplitSettings
  ): String = {
    val identifiers = key.map(_.identifier).mkString(", ")
    val conditions = filters.flatMap(IndexConditions(key, _))
    val rows = "sum(rows_in_granule) OVER ()"
    val bySize = s"greatest(${settings.atLeast}, toUInt64(ceil($rows / ${settings.targetRowsPerTask})))"
    val slices = settings.maxParallelism.fold(bySize)(most => s"least($most, $bySize)")
    s"""SELECT ${key.map(entry => entry.keyType.read(entry.identifier)).mkString(", ")}
       |FROM (
       |  SELECT $identifiers,
       |    sum(rows_in_granule) OVER (ORDER BY $identifiers ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
       |      - rows_in_granule AS _rows_before,
       |    intDiv(_rows_before * $slices, $rows) AS _slice
       |  FROM mergeTreeIndex(${SqlText.quote(table.database)}, ${SqlText.quote(table.name)})
       |  WHERE ${("rows_in_granule > 0" +: conditions.map(c => s"($c)")).mkString(" AND ")}
       |)
       |ORDER BY _rows_before
       |LIMIT 1 BY _slice
       |""".stripMargin
  }
}
