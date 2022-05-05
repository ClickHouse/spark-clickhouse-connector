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

package xenon.clickhouse

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.sources._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.Utils._

trait SQLHelper {

  def quoted(token: String) = s"`$token`"

  // null => null, ' => ''
  def escapeSql(value: String): String = StringUtils.replace(value, "'", "''")

  def compileValue(value: Any)(implicit tz: ZoneId): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case utf8: UTF8String => s"'${escapeSql(utf8.toString)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case timestampValue: Instant => s"'${dateTimeFmt.withZone(tz).format(timestampValue)}'"
    case dateValue: Date => "'" + dateValue + "'"
    case dateValue: LocalDate => s"'${dateFmt.format(dateValue)}'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  def compileFilter(f: Filter)(implicit tz: ZoneId): Option[String] = Option(f match {
    case AlwaysTrue => "1=1"
    case AlwaysFalse => "1=0"
    case EqualTo(attr, value) => s"${quoted(attr)} = ${compileValue(value)}"
    case EqualNullSafe(attr, nullableValue) =>
      val (col, value) = (quoted(attr), compileValue(nullableValue))
      s"(NOT ($col != $value OR $col IS NULL OR $value IS NULL) OR ($col IS NULL AND $value IS NULL))"
    case LessThan(attr, value) => s"${quoted(attr)} < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"${quoted(attr)} > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"${quoted(attr)} <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"${quoted(attr)} >= ${compileValue(value)}"
    case IsNull(attr) => s"${quoted(attr)} IS NULL"
    case IsNotNull(attr) => s"${quoted(attr)} IS NOT NULL"
    case StringStartsWith(attr, value) => s"${quoted(attr)} LIKE '$value%'"
    case StringEndsWith(attr, value) => s"${quoted(attr)} LIKE '%$value'"
    case StringContains(attr, value) => s"${quoted(attr)} LIKE '%$value%'"
    case In(attr, value) if value.isEmpty => s"CASE WHEN ${quoted(attr)} IS NULL THEN NULL ELSE FALSE END"
    case In(attr, value) => s"${quoted(attr)} IN (${compileValue(value)})"
    case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").orNull
    case Or(f1, f2) =>
      val or = Seq(f1, f2).flatMap(_f => compileFilter(_f)(tz))
      if (or.size == 2) or.map(p => s"($p)").mkString(" OR ") else null
    case And(f1, f2) =>
      val and = Seq(f1, f2).flatMap(_f => compileFilter(_f)(tz))
      if (and.size == 2) and.map(p => s"($p)").mkString(" AND ") else null
    case _ => null
  })

  def compileAggregate(aggFunction: AggregateFunc): Option[String] =
    aggFunction match {
      case min: Min if min.column.isInstanceOf[NamedReference] =>
        val col = min.column.asInstanceOf[NamedReference]
        if (col.fieldNames().length != 1) return None
        Some(s"MIN(${quoted(col.fieldNames.head)})")
      case max: Max if max.column.isInstanceOf[NamedReference] =>
        val col = max.column.asInstanceOf[NamedReference]
        if (col.fieldNames.length != 1) return None
        Some(s"MAX(${quoted(col.fieldNames.head)})")
      case count: Count if count.column.isInstanceOf[NamedReference] =>
        val col = count.column.asInstanceOf[NamedReference]
        if (col.fieldNames.length != 1) return None
        val distinct = if (count.isDistinct) "DISTINCT " else ""
        val column = quoted(col.fieldNames.head)
        Some(s"COUNT($distinct$column)")
      case sum: Sum if sum.column.isInstanceOf[NamedReference] =>
        val col = sum.column.asInstanceOf[NamedReference]
        if (col.fieldNames.length != 1) return None
        val distinct = if (sum.isDistinct) "DISTINCT " else ""
        val column = quoted(col.fieldNames.head)
        Some(s"SUM($distinct$column)")
      case _: CountStar =>
        Some("COUNT(*)")
      case _ => None
    }

  def compileFilters(filters: Seq[Filter])(implicit tz: ZoneId): String =
    filters
      .flatMap(_f => compileFilter(_f)(tz))
      .map(p => s"($p)").mkString(" AND ")
}
