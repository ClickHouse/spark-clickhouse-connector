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

import com.clickhouse.spark.Utils
import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.exception.ClickHouseErrCode
import com.clickhouse.spark.expr.PushedFilter

import scala.util.control.NonFatal

/**
 * A `[lower, upper)` range of a primary key prefix, compared lexicographically. A missing bound is
 * open-ended, so the ranges planned for a table cover every key value exactly once.
 */
case class PrimaryKeyRange(columns: Seq[String], lower: Option[Seq[String]], upper: Option[Seq[String]]) {

  def filterExpr: String = {
    val bounds = lower.map(bound(_, ">", ">=")).toSeq ++ upper.map(bound(_, "<", "<"))
    if (bounds.isEmpty) "1=1" else bounds.map(b => s"($b)").mkString(" AND ")
  }

  // expanded into `k1 > v1 OR (k1 = v1 AND ...)`, as ClickHouse's key analysis ignores tuple comparisons
  private def bound(values: Seq[String], op: String, lastOp: String): String = {
    def render(terms: List[(String, String)]): String = terms match {
      case (column, value) :: Nil => s"$column $lastOp $value"
      case (column, value) :: rest =>
        val inner = if (rest.length > 1) s"(${render(rest)})" else render(rest)
        s"$column $op $value OR ($column = $value AND $inner)"
    }
    render(columns.map(Utils.wrapBackQuote).zip(values).toList)
  }
}

/** Why a table can't be split by its primary key, with the failure when planning threw. */
case class NotSplittable(reason: String, cause: Option[Throwable] = None)

object PrimaryKeyRange {

  /** How a key column's cut values are read from the index and written back as SQL literals. */
  sealed private[spec] trait KeyType {
    def read(column: String): String
    def literal(value: String): String
  }

  private[spec] case object IntegerKey extends KeyType {
    def read(column: String): String = s"toString($column)"
    def literal(value: String): String = {
      require(value.matches("-?\\d+"), s"Not an integer: $value")
      value
    }
  }

  // Date, Date32 and UUID round-trip through their string form
  private[spec] case object StringFormKey extends KeyType {
    def read(column: String): String = s"toString($column)"
    def literal(value: String): String = quote(value)
  }

  // in UTC, so a cut does not depend on the server's or the column's time zone
  private[spec] case class DateTimeKey(precision: Option[Int]) extends KeyType {
    def read(column: String): String = s"toString($column, 'UTC')"
    def literal(value: String): String = precision match {
      case Some(p) => s"toDateTime64(${quote(value)}, $p, 'UTC')"
      case None => s"toDateTime(${quote(value)}, 'UTC')"
    }
  }

  // hex-encoded, as the JSON output does not carry arbitrary bytes
  private[spec] case object BytesKey extends KeyType {
    def read(column: String): String = s"hex($column)"
    def literal(value: String): String = bytesLiteral(value)
  }

  private val IntegerType = "U?Int(?:8|16|32|64)".r
  private val DateTime64Type = "DateTime64\\((\\d+).*\\)".r
  private val DateTimeType = "DateTime(?:\\(.*\\))?".r
  private val FixedStringType = "FixedString\\(\\d+\\)".r
  private val LowCardinalityType = "LowCardinality\\((.*)\\)".r

  /** The key types ranges can be cut on. NULL and NaN fall outside every range, so Nullable and floats can't. */
  private[spec] def keyType(clickHouseType: String): Option[KeyType] = clickHouseType match {
    case LowCardinalityType(inner) => keyType(inner)
    case IntegerType() => Some(IntegerKey)
    case "Date" | "Date32" | "UUID" => Some(StringFormKey)
    case DateTime64Type(precision) => Some(DateTimeKey(Some(precision.toInt)))
    case DateTimeType() => Some(DateTimeKey(None))
    case "String" | FixedStringType() => Some(BytesKey)
    case _ => None
  }

  private def quote(value: String): String = s"'${value.replace("\\", "\\\\").replace("'", "\\'")}'"

  // a string literal of hex-encoded bytes, escaping all but printable ASCII
  private[spec] def bytesLiteral(hex: String): String = {
    require(hex.length % 2 == 0 && hex.forall(Character.digit(_, 16) >= 0), s"Not a hex string: $hex")
    hex.grouped(2).map(Integer.parseInt(_, 16).toChar).map {
      case c @ ('\'' | '\\') => s"\\$c"
      case c if c >= ' ' && c <= '~' => c.toString
      case c => f"\\x${c.toInt}%02X"
    }.mkString("'", "", "'")
  }

  // `mergeTreeIndex` returns these, then the primary key columns
  private val IndexColumns = Set("part_name", "mark_number", "rows_in_granule")

  // longer cut values are skipped, as each range repeats its bounds in every scan query
  private val MaxLiteralLength = 1024

  /**
   * Splits `table` into ranges of its primary key holding about the same number of rows, cut from the primary index
   * without reading data: two at least, up to `slices`. Or why the table can't be split by its primary key.
   *
   * @param tableColumns
   *   the table's columns, telling key columns from key expressions
   * @param filters
   *   the pushed-down filters; those on key columns limit the cuts to the rows read
   */
  def plan(
    table: TableSpec,
    tableColumns: Set[String],
    filters: Seq[PushedFilter],
    slices: Int
  )(implicit nodeClient: NodeClient): Either[NotSplittable, Seq[PrimaryKeyRange]] =
    if (!table.engine.endsWith("MergeTree")) Left(NotSplittable(s"the ${table.engine} engine has no primary index"))
    else if (table.primary_key.isEmpty) Left(NotSplittable("the table has no primary key"))
    else
      try describeIndex(table) match {
          case None => Left(NotSplittable("reading the primary index needs ClickHouse 24.2+"))
          case Some(indexColumns) =>
            val key = splitColumns(indexColumns, tableColumns)
            if (key.isEmpty) {
              Left(NotSplittable(
                "the primary key does not start with a column of an integer, date, date time, string or UUID type"
              ))
            } else {
              val cuts = cutPoints(table, key, filters, slices)
              if (cuts.isEmpty) Left(NotSplittable("the primary index yields no cut point within the filters"))
              else Right(ranges(key.map(_._1), cuts))
            }
        }
      catch {
        case NonFatal(e) => Left(NotSplittable("planning the primary key ranges failed", Some(e)))
      }

  // None before ClickHouse 24.2, which lacks `mergeTreeIndex`
  private def describeIndex(table: TableSpec)(implicit nodeClient: NodeClient): Option[Seq[(String, String)]] =
    nodeClient.syncQueryOutputJSONEachRow(
      s"DESCRIBE TABLE mergeTreeIndex(${quote(table.database)}, ${quote(table.name)})"
    ) match {
      case Left(e) if e.code == ClickHouseErrCode.UNKNOWN_FUNCTION.code() => None
      case Left(e) => throw e
      case Right(output) => Some(output.records.toList.map(row => row.get("name").asText -> row.get("type").asText))
    }

  /** The longest prefix of the primary key made of table columns of a supported [[keyType]]. */
  private[spec] def splitColumns(
    indexColumns: Seq[(String, String)],
    tableColumns: Set[String]
  ): Seq[(String, KeyType)] =
    indexColumns
      .filterNot { case (column, _) => IndexColumns.contains(column) }
      .map { case (column, chType) => if (tableColumns.contains(column)) keyType(chType).map(column -> _) else None }
      .takeWhile(_.isDefined)
      .flatten

  /** The keys to cut at, in key order. The smallest is dropped, as cutting there would leave the first range empty. */
  private def cutPoints(
    table: TableSpec,
    key: Seq[(String, KeyType)],
    filters: Seq[PushedFilter],
    slices: Int
  )(implicit nodeClient: NodeClient): Seq[Seq[String]] =
    nodeClient.syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(cutsQuery(table, key, filters, slices))
      .records.toList
      .map(row => key.zip(row).map { case ((_, keyType), value) => keyType.literal(value.asText) })
      .distinct
      .drop(1)
      .filterNot(_.exists(_.length > MaxLiteralLength))

  /** The `[lower, upper)` ranges between consecutive cuts, open-ended at both ends. */
  private def ranges(columns: Seq[String], cuts: Seq[Seq[String]]): Seq[PrimaryKeyRange] = {
    val bounds = cuts.map(Some(_))
    (None +: bounds).zip(bounds :+ None).map { case (lower, upper) => PrimaryKeyRange(columns, lower, upper) }
  }

  /**
   * Orders the granules the filters keep by the key they start at and returns the key of the first granule in each
   * `1/slices` of their rows, so ranges cut there hold about as many rows each and none less than a granule.
   */
  private[spec] def cutsQuery(
    table: TableSpec,
    key: Seq[(String, KeyType)],
    filters: Seq[PushedFilter],
    slices: Int
  ): String = {
    val columns = key.map { case (column, _) => Utils.wrapBackQuote(column) }
    val keyColumns = key.map(_._1).toSet
    // key columns after a high-cardinality prefix may be left unloaded, reading as their default: keep those granules
    val keyFilters = filters.collect {
      case PushedFilter(sql, columns) if columns.subsetOf(keyColumns) =>
        val unloaded = (columns - key.head._1).toSeq.sorted.map(Utils.wrapBackQuote)
          .map(column => s"$column = defaultValueOfArgumentType($column)")
        (s"($sql)" +: unloaded).mkString(" OR ")
    }
    s"""SELECT ${key.map { case (column, keyType) => keyType.read(Utils.wrapBackQuote(column)) }.mkString(", ")}
       |FROM (
       |  SELECT ${columns.mkString(", ")},
       |    sum(rows_in_granule) OVER (ORDER BY ${columns.mkString(
        ", "
      )} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
       |      - rows_in_granule AS _rows_before,
       |    intDiv(_rows_before * $slices, sum(rows_in_granule) OVER ()) AS _slice
       |  FROM mergeTreeIndex(${quote(table.database)}, ${quote(table.name)})
       |  WHERE ${("rows_in_granule > 0" +: keyFilters.map(f => s"($f)")).mkString(" AND ")}
       |)
       |ORDER BY _rows_before
       |LIMIT 1 BY _slice
       |""".stripMargin
  }
}
