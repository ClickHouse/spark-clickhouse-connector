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

import com.clickhouse.spark.Utils
import com.clickhouse.spark.expr.PushedFilter

/**
 * An entry of the split key. The column of `mergeTreeIndex` named `name` holds its values; `sql` is how a task's
 * filter writes it against the table.
 */
sealed private[primaryindex] trait KeyEntry {
  def name: String
  def sql: String
  def keyType: KeyType

  /** In the cut query, where the entry is a column of the index. */
  def identifier: String = Utils.wrapBackQuote(name)

  /** A comparison or IN filter as a condition on this entry's index values, when it constrains them. */
  def indexCondition(filter: PushedFilter): Option[String]
}

/** A table column, written back-quoted. */
private[primaryindex] case class ColumnEntry(name: String, keyType: KeyType) extends KeyEntry {
  def sql: String = Utils.wrapBackQuote(name)

  def indexCondition(filter: PushedFilter): Option[String] = filter match {
    case PushedFilter.Compare(`name`, op, value) => Some(s"$identifier $op $value")
    case PushedFilter.In(`name`, values) => Some(s"$identifier IN (${values.mkString(",")})")
    case _ => None
  }
}

/** A monotonic time function of a table column, so a filter on that column constrains it. */
private[primaryindex] case class MonotonicEntry(name: String, sql: String, keyType: KeyType, function: Monotonic)
    extends KeyEntry {

  def indexCondition(filter: PushedFilter): Option[String] = filter match {
    case PushedFilter.Compare(function.column, op, value) => Some(s"$identifier ${relaxed(op)} ${function(value)}")
    case PushedFilter.In(function.column, values) =>
      Some(s"$identifier IN (${values.map(function.apply).mkString(", ")})")
    case _ => None
  }

  // a monotonic function repeats values, so `ts < X` only implies `f(ts) <= f(X)`
  private def relaxed(op: String): String = op match {
    case "<" => "<="
    case ">" => ">="
    case other => other
  }
}

/** Any other expression: cut on, but no filter constrains it. */
private[primaryindex] case class OpaqueEntry(name: String, sql: String, keyType: KeyType) extends KeyEntry {
  def indexCondition(filter: PushedFilter): Option[String] = None
}

private[primaryindex] object KeyEntry {

  /**
   * The entry the index column `name` of `chType` is, `spelling` being ClickHouse's formatting of it in the primary
   * key. None when ranges can't be cut on its type.
   */
  def of(name: String, chType: String, spelling: String, tableColumns: Map[String, String]): Option[KeyEntry] =
    KeyType.of(chType).map { keyType =>
      // parenthesized, as an entry like `a AND b` binds looser than the comparison of a range bound
      val sql = s"($spelling)"
      if (tableColumns.contains(name)) ColumnEntry(name, keyType)
      else if (keyType == KeyType.BytesKey) OpaqueEntry(name, sql, keyType)
      else Monotonic.of(spelling, tableColumns).fold[KeyEntry](OpaqueEntry(name, sql, keyType)) { function =>
        MonotonicEntry(name, sql, keyType, function)
      }
    }
}

/** `f(column, constants...)`: applies `f` to a literal of `column`, cast to the column's type. */
private[primaryindex] case class Monotonic(column: String, columnType: String, before: String, after: String) {
  def apply(literal: String): String = s"${before}CAST($literal AS $columnType)$after"
}

private[primaryindex] object Monotonic {

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
  def of(expression: String, tableColumns: Map[String, String]): Option[Monotonic] =
    expression match {
      case Call(function, argList) if MonotonicTimeFunctions.contains(function) =>
        SqlText.splitArgs(argList).flatMap { args =>
          args.indices.filter(i => tableColumns.contains(SqlText.unquote(args(i)))) match {
            case Seq(i) if TimeOrInteger.pattern.matcher(tableColumns(SqlText.unquote(args(i)))).matches =>
              val column = SqlText.unquote(args(i))
              val before = s"$function(" + args.take(i).map(_ + ", ").mkString
              val after = args.drop(i + 1).map(", " + _).mkString + ")"
              Some(Monotonic(column, tableColumns(column), before, after))
            case _ => None
          }
        }
      case _ => None
    }
}
