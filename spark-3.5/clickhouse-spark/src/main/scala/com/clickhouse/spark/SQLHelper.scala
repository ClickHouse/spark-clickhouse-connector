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

package com.clickhouse.spark

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.sources._
import org.apache.spark.unsafe.types.UTF8String
import Utils._

trait SQLHelper {

  def quoted(token: String): String = token match {
    case t if t.startsWith("`") && t.endsWith("`") => t
    case t => s"`$t`"
  }

  // null => null, ' => ''
  def escapeSql(value: String): String = StringUtils.replace(value, "'", "''")

  def compileValue(value: Any): Any = value match {
    case string: String => s"'${escapeSql(string)}'"
    case utf8: UTF8String => s"'${escapeSql(utf8.toString)}'"
    case instant: Instant => compileInstant(instant)
    case timestamp: Timestamp => compileInstant(timestamp.toInstant)
    case localDateTime: LocalDateTime => s"'${dateTimeFmt.format(localDateTime)}'"
    // a java.sql.Date is a calendar date whose JVM-default-zone rendering is that date, not an instant
    case legacyDate: Date => s"'${legacyDateFmt.format(legacyDate)}'"
    case localDate: LocalDate => s"'${dateFmt.format(localDate)}'"
    case array: Array[Any] => array.map(compileValue).mkString(",")
    case _ => value
  }

  /**
   * Renders a timestamp literal as a UTC `DateTime64`. ClickHouse reads a plain string literal in the column's own
   * time zone (a `DateTime('Asia/Tokyo')` column reads it as Tokyo time), so a string is only right when that zone
   * happens to be the catalog's. A typed UTC literal compares by instant with `DateTime` and `DateTime64` columns in
   * any zone, and keeps sub-second and pre-1970 values.
   */
  private def compileInstant(instant: Instant): String =
    s"toDateTime64('${utcDateTimeFmt.format(instant)}', 6, 'UTC')"

  // DateTimeFormatter isn't Serializable, and the scans that mix this in are serialized
  @transient private lazy val utcDateTimeFmt = dateTimeFmt.withZone(ZoneOffset.UTC)

  // the last instant a `DateTime` (UInt32 seconds) can hold: 2106-02-07 06:28:15 UTC
  private val maxDateTime = Instant.ofEpochSecond(4294967295L)

  private def instantOf(value: Any): Option[Instant] = value match {
    case instant: Instant => Some(instant)
    case timestamp: Timestamp => Some(timestamp.toInstant)
    case _ => None
  }

  /**
   * An `IN` list element. ClickHouse converts each `IN` element to the column's type, and rejects a `DateTime64`
   * element for a `DateTime` column, so a timestamp is a UTC `DateTime` literal here. That holds only whole seconds
   * from 1970 to 2106; see [[pushableIn]].
   */
  private def compileInValue(value: Any): Any = instantOf(value) match {
    case Some(instant) => s"toDateTime('${utcDateTimeFmt.format(instant)}', 'UTC')"
    case None => compileValue(value)
  }

  private def pushableIn(values: Array[Any]): Boolean = values.flatMap(instantOf).forall { instant =>
    instant.getNano == 0 && !instant.isBefore(Instant.EPOCH) && !instant.isAfter(maxDateTime)
  }

  def compileFilter(f: Filter): Option[String] = Option(f match {
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
    // a timestamp that a `DateTime` literal cannot hold exactly is left to Spark, see compileInValue
    case In(_, value) if !pushableIn(value) => null
    case In(attr, value) => s"${quoted(attr)} IN (${value.map(compileInValue).mkString(",")})"
    case Not(f) => compileFilter(f).map(p => s"(NOT ($p))").orNull
    case Or(f1, f2) =>
      val or = Seq(f1, f2).flatMap(_f => compileFilter(_f))
      if (or.size == 2) or.map(p => s"($p)").mkString(" OR ") else null
    case And(f1, f2) =>
      val and = Seq(f1, f2).flatMap(_f => compileFilter(_f))
      if (and.size == 2) and.map(p => s"($p)").mkString(" AND ") else null
    case _ => null
  })

  /**
   * ClickHouse answers an aggregate over no rows with the column's default rather than NULL (`0`,
   * `[]`, a tuple of defaults), and Spark folds a task's partial result into the final value, so a
   * task that matches nothing must not contribute one. The caller arranges that an empty task
   * returns no row at all wherever it can - a GROUP BY returns only the groups that have rows, and
   * without one the query gets `HAVING count() > 0` - and the plain aggregates are then exact and
   * keep their types. That is not possible with a `COUNT`, whose correct empty-set value is a 0
   * row that Spark needs, so there [[nullIfEmpty]] makes the other aggregates NULL in that row.
   *
   * @param nullIfEmpty
   *   whether an empty task still returns a row. `MIN`/`MAX` are then sent as `if(count() = 0,
   *   NULL, min(col))`, which, unlike `minOrNull`, can still be answered from
   *   `_minmax_count_projection`, and `SUM` as `sumOrNull`. `COUNT` keeps its plain form: Spark
   *   merges it with a `Sum` whose output attribute is non-nullable, so a NULL there corrupts it.
   * @param containerColumns
   *   Array, Map and Tuple columns, whose `MIN`/`MAX` is not pushed when `nullIfEmpty`: ClickHouse
   *   cannot make a container Nullable before 26.3, and for Array and Map the NULL form is a
   *   `Variant` after it.
   */
  def compileAggregate(
    aggFunction: AggregateFunc,
    nullIfEmpty: Boolean = false,
    containerColumns: Set[String] = Set.empty
  ): Option[String] =
    aggFunction match {
      case min: Min if min.column.isInstanceOf[NamedReference] =>
        val col = min.column.asInstanceOf[NamedReference]
        if (col.fieldNames().length != 1) return None
        compileMinMax("MIN", col.fieldNames.head, nullIfEmpty, containerColumns)
      case max: Max if max.column.isInstanceOf[NamedReference] =>
        val col = max.column.asInstanceOf[NamedReference]
        if (col.fieldNames.length != 1) return None
        compileMinMax("MAX", col.fieldNames.head, nullIfEmpty, containerColumns)
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
        Some(s"${if (nullIfEmpty) "sumOrNull" else "SUM"}($distinct$column)")
      case _: CountStar =>
        Some("COUNT(*)")
      case _ => None
    }

  /** See [[compileAggregate]]. */
  private def compileMinMax(
    function: String,
    column: String,
    nullIfEmpty: Boolean,
    containerColumns: Set[String]
  ): Option[String] = {
    val plain = s"$function(${quoted(column)})"
    if (!nullIfEmpty) Some(plain)
    else if (containerColumns.contains(column)) None
    else Some(s"if(count() = 0, NULL, $plain)")
  }

  def compileFilters(filters: Seq[Filter]): String =
    filters
      .flatMap(_f => compileFilter(_f))
      .map(p => s"($p)").mkString(" AND ")
}
