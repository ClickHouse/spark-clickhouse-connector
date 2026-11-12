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
 * A `[lower, upper)` range of a primary key prefix, compared lexicographically. `key` holds the entries as a task's
 * filter writes them: a back-quoted column, or an expression as it is. A missing bound is open-ended, so the ranges
 * planned for a table cover every key value exactly once.
 */
case class PrimaryKeyRange(key: Seq[String], lower: Option[Seq[String]], upper: Option[Seq[String]]) {

  def filterExpr: String = {
    val bounds = lower.map(bound(_, ">", ">=")).toSeq ++ upper.map(bound(_, "<", "<"))
    if (bounds.isEmpty) "1=1" else bounds.map(b => s"($b)").mkString(" AND ")
  }

  // expanded into `k1 > v1 OR (k1 = v1 AND ...)`, as ClickHouse's key analysis ignores tuple comparisons
  private def bound(values: Seq[String], op: String, lastOp: String): String = {
    def render(terms: List[(String, String)]): String = terms match {
      case (entry, value) :: Nil => s"$entry $lastOp $value"
      case (entry, value) :: rest =>
        val inner = if (rest.length > 1) s"(${render(rest)})" else render(rest)
        s"$entry $op $value OR ($entry = $value AND $inner)"
    }
    render(key.zip(values).toList)
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
   * An entry of the split key. The column of `mergeTreeIndex` named `name` holds its values; `sql` is how a task's
   * filter writes it against the table: a back-quoted column, or the expression as ClickHouse itself formats it in
   * the table's primary key. `monotonic` is set when the entry is a monotonic time function of a table column, so
   * that a filter on that column can place the cuts.
   */
  private[spec] case class KeyEntry(
    name: String,
    sql: String,
    keyType: KeyType,
    isColumn: Boolean,
    monotonic: Option[Monotonic]
  ) {

    /** In the cut query, where the entry is a column of the index. */
    def identifier: String = Utils.wrapBackQuote(name)
  }

  /** `f(column, constants...)`: applies `f` to a literal of `column`, cast to the column's type. */
  private[spec] case class Monotonic(column: String, columnType: String, before: String, after: String) {
    def apply(literal: String): String = s"${before}CAST($literal AS $columnType)$after"
  }

  /**
   * ClickHouse functions that never decrease as their date, time or timestamp argument grows, so that a range of the
   * argument maps to a range of the function. Periodic ones such as toHour, toDayOfWeek or toMonth do not qualify.
   */
  private val MonotonicTimeFunctions = Set(
    "toStartOfYear",
    "toStartOfISOYear",
    "toStartOfQuarter",
    "toStartOfMonth",
    "toLastDayOfMonth",
    "toMonday",
    "toStartOfWeek",
    "toLastDayOfWeek",
    "toStartOfDay",
    "toStartOfHour",
    "toStartOfMinute",
    "toStartOfSecond",
    "toStartOfMillisecond",
    "toStartOfMicrosecond",
    "toStartOfNanosecond",
    "toStartOfFiveMinutes",
    "toStartOfFiveMinute",
    "toStartOfTenMinutes",
    "toStartOfFifteenMinutes",
    "toStartOfInterval",
    "dateTrunc",
    "date_trunc",
    "toYear",
    "toISOYear",
    "toYYYYMM",
    "toYYYYMMDD",
    "toYYYYMMDDhhmmss",
    "toDate",
    "toDate32",
    "toDateTime",
    "toDateTime64",
    "toTimezone",
    "toTimeZone",
    "toUnixTimestamp",
    "toUnixTimestamp64Second",
    "toUnixTimestamp64Milli",
    "toUnixTimestamp64Micro",
    "toUnixTimestamp64Nano",
    "fromUnixTimestamp",
    "fromUnixTimestamp64Second",
    "fromUnixTimestamp64Milli",
    "fromUnixTimestamp64Micro",
    "fromUnixTimestamp64Nano",
    "toRelativeYearNum",
    "toRelativeQuarterNum",
    "toRelativeMonthNum",
    "toRelativeWeekNum",
    "toRelativeDayNum",
    "toRelativeHourNum",
    "toRelativeMinuteNum",
    "toRelativeSecondNum",
    "addWeeks",
    "addDays",
    "addHours",
    "addMinutes",
    "addSeconds",
    "subtractWeeks",
    "subtractDays",
    "subtractHours",
    "subtractMinutes",
    "subtractSeconds"
  )

  // the column types the functions above are monotonic over; on a String they follow string order instead
  private val TimeOrInteger =
    "(?:LowCardinality\\()?(?:Date|Date32|DateTime(?:64)?(?:\\(.*\\))?|U?Int(?:8|16|32|64))\\)?".r

  private val Call = "(\\w+)\\((.*)\\)".r

  /**
   * The monotonic time function of one date, time or integer table column that `expression` is, its constant
   * arguments kept. `expression` is ClickHouse's own formatting of the key entry.
   */
  private[spec] def monotonicOf(expression: String, tableColumns: Map[String, String]): Option[Monotonic] =
    expression match {
      case Call(function, argList) if MonotonicTimeFunctions.contains(function) =>
        splitArgs(argList).flatMap { args =>
          args.indices.filter(i => tableColumns.contains(unquote(args(i)))) match {
            case Seq(i) if TimeOrInteger.pattern.matcher(tableColumns(unquote(args(i)))).matches =>
              val column = unquote(args(i))
              val before = s"$function(" + args.take(i).map(_ + ", ").mkString
              val after = args.drop(i + 1).map(", " + _).mkString + ")"
              Some(Monotonic(column, tableColumns(column), before, after))
            case _ => None
          }
        }
      case _ => None
    }

  private def unquote(identifier: String): String =
    if (identifier.length > 1 && identifier.startsWith("`") && identifier.endsWith("`"))
      identifier.substring(1, identifier.length - 1)
    else identifier

  // the arguments of a call, split at the commas outside parentheses and quotes; None when unbalanced
  private def splitArgs(argList: String): Option[Seq[String]] = {
    val args = Seq.newBuilder[String]
    val current = new StringBuilder
    var depth = 0
    var quote: Option[Char] = None
    var i = 0
    while (i < argList.length && depth >= 0) {
      val c = argList(i)
      quote match {
        case Some(q) =>
          current += c
          if (c == '\\' && i + 1 < argList.length) {
            current += argList(i + 1)
            i += 1
          } else if (c == q) quote = None
        case None => c match {
            case '\'' | '"' | '`' =>
              quote = Some(c)
              current += c
            case '(' =>
              depth += 1
              current += c
            case ')' =>
              depth -= 1
              current += c
            case ',' if depth == 0 =>
              args += current.toString.trim
              current.clear()
            case _ => current += c
          }
      }
      i += 1
    }
    args += current.toString.trim
    if (depth == 0 && quote.isEmpty) Some(args.result()) else None
  }

  /**
   * Splits `table` into ranges of its primary key holding about the same number of rows, cut from the primary index
   * without reading data: two at least, up to `slices`. Or why the table can't be split by its primary key.
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
    slices: Int
  )(implicit nodeClient: NodeClient): Either[NotSplittable, Seq[PrimaryKeyRange]] =
    try
      for {
        _ <- check(table.engine.endsWith("MergeTree"), s"the ${table.engine} engine has no primary index")
        _ <- check(table.primary_key.nonEmpty, "the table has no primary key")
        indexColumns <- describeIndex(table).toRight(NotSplittable("reading the primary index needs ClickHouse 24.2+"))
        primaryKey <- splitArgs(table.primary_key).toRight(NotSplittable("the primary key can't be read"))
        _ <- check(
          indexColumns.count { case (name, _) => !IndexColumns.contains(name) } == primaryKey.size,
          "the primary index does not list the primary key"
        )
        key = splitColumns(indexColumns, primaryKey, tableColumns)
        _ <- check(
          key.nonEmpty,
          "the primary key does not start with an integer, date, date time, string or UUID column or expression"
        )
        cuts = cutPoints(table, key, filters, slices)
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
      s"DESCRIBE TABLE mergeTreeIndex(${quote(table.database)}, ${quote(table.name)})"
    ) match {
      case Left(e) if e.code == ClickHouseErrCode.UNKNOWN_FUNCTION.code() => None
      case Left(e) => throw e
      case Right(output) => Some(output.records.toList.map(row => row.get("name").asText -> row.get("type").asText))
    }

  /**
   * The longest prefix of the primary key of supported [[keyType]]s, columns and expressions alike. `primaryKey`
   * spells the entries as ClickHouse formats them, in the order of the index columns, which name them with their
   * identifiers unquoted.
   */
  private[spec] def splitColumns(
    indexColumns: Seq[(String, String)],
    primaryKey: Seq[String],
    tableColumns: Map[String, String]
  ): Seq[KeyEntry] =
    indexColumns
      .filterNot { case (name, _) => IndexColumns.contains(name) }
      .zip(primaryKey)
      .map { case ((name, chType), spelling) =>
        keyType(chType).map { keyType =>
          val isColumn = tableColumns.contains(name)
          val sql = if (isColumn) Utils.wrapBackQuote(name) else spelling
          val monotonic = if (isColumn || keyType == BytesKey) None else monotonicOf(spelling, tableColumns)
          KeyEntry(name, sql, keyType, isColumn, monotonic)
        }
      }
      .takeWhile(_.isDefined)
      .flatten

  /** The keys to cut at, in key order. The smallest is dropped, as cutting there would leave the first range empty. */
  private def cutPoints(
    table: TableSpec,
    key: Seq[KeyEntry],
    filters: Seq[PushedFilter],
    slices: Int
  )(implicit nodeClient: NodeClient): Seq[Seq[String]] =
    nodeClient.syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(cutsQuery(table, key, filters, slices))
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
   */
  private[spec] def cutsQuery(
    table: TableSpec,
    key: Seq[KeyEntry],
    filters: Seq[PushedFilter],
    slices: Int
  ): String = {
    val identifiers = key.map(_.identifier).mkString(", ")
    val conditions = filters.flatMap(indexConditions(key, _))
    s"""SELECT ${key.map(entry => entry.keyType.read(entry.identifier)).mkString(", ")}
       |FROM (
       |  SELECT $identifiers,
       |    sum(rows_in_granule) OVER (ORDER BY $identifiers ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
       |      - rows_in_granule AS _rows_before,
       |    intDiv(_rows_before * $slices, sum(rows_in_granule) OVER ()) AS _slice
       |  FROM mergeTreeIndex(${quote(table.database)}, ${quote(table.name)})
       |  WHERE ${("rows_in_granule > 0" +: conditions.map(c => s"($c)")).mkString(" AND ")}
       |)
       |ORDER BY _rows_before
       |LIMIT 1 BY _slice
       |""".stripMargin
  }

  /**
   * A pushed filter as conditions on the index rows, which hold the key only. A filter on key columns applies as it
   * is. A comparison of a column with a monotonic time function of it in the key applies to the function's value,
   * strict comparisons relaxed as the function repeats values. Other filters apply in the tasks only.
   */
  private def indexConditions(key: Seq[KeyEntry], filter: PushedFilter): Seq[String] = {
    val columnEntries = key.filter(_.isColumn)
    // key entries after a high-cardinality first entry may be left unloaded, reading as their default: keep those
    def on(entries: Seq[KeyEntry])(condition: String): String = {
      val unloaded = entries.filterNot(_ == key.head).map(_.identifier).distinct.sorted
        .map(id => s"$id = defaultValueOfArgumentType($id)")
      (s"($condition)" +: unloaded).mkString(" OR ")
    }
    filter match {
      case PushedFilter.Other(sql, columns) if columns.subsetOf(columnEntries.map(_.name).toSet) =>
        Seq(on(columnEntries.filter(entry => columns.contains(entry.name)))(sql))
      case PushedFilter.Compare(column, op, value) => key.collect {
          case entry if entry.isColumn && entry.name == column => on(Seq(entry))(s"${entry.identifier} $op $value")
          case entry if entry.monotonic.exists(_.column == column) =>
            on(Seq(entry))(s"${entry.identifier} ${relaxed(op)} ${entry.monotonic.get(value)}")
        }
      case PushedFilter.In(column, values) => key.collect {
          case entry if entry.isColumn && entry.name == column =>
            on(Seq(entry))(s"${entry.identifier} IN (${values.mkString(",")})")
          case entry if entry.monotonic.exists(_.column == column) =>
            on(Seq(entry))(s"${entry.identifier} IN (${values.map(entry.monotonic.get.apply).mkString(", ")})")
        }
      case _ => Nil
    }
  }

  // a monotonic function repeats values, so `ts < X` only implies `f(ts) <= f(X)`
  private def relaxed(op: String): String = op match {
    case "<" => "<="
    case ">" => ">="
    case other => other
  }
}
