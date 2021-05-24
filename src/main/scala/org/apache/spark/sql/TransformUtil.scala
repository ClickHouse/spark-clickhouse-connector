package org.apache.spark.sql

import scala.util.matching.Regex

import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.expressions._

object TransformUtil {
  // Some functions of ClickHouse which match Spark pre-defined Transforms
  //
  // toYear     - Converts a date or date with time to a UInt16 (AD)                       Alias: YEAR
  // toYYYYMM   - Converts a date or date with time to a UInt32 (YYYY*100 + MM)
  // toYYYYMMDD - Converts a date or date with time to a UInt32 (YYYY*10000 + MM*100 + DD)
  // toHour     - Converts a date with time to a         UInt8  (0-23)                     Alias: HOUR

  // format: off
  private[sql] val years_transform_regex:  Regex = """toYear\((\w+)\)""".r
  private[sql] val months_transform_regex: Regex = """toYYYYMM\((\w+)\)""".r
  private[sql] val days_transform_regex:   Regex = """toYYYYMMDD\((\w+)\)""".r
  private[sql] val hours_transform_regex:  Regex = """toHour\((\w+)\)""".r
  private[sql] val identity_regex:         Regex = """^(\w+)$""".r
  // format: on

  def fromClickHouse(transformExpr: String): Transform =
    transformExpr match {
      case years_transform_regex(expr) => years(expr)
      case months_transform_regex(expr) => months(expr)
      case days_transform_regex(expr) => days(expr)
      case hours_transform_regex(expr) => hours(expr)
      // Assume all others is just a column name without any transforms,
      // thus, `xxHash64(abc)` is not supported yet.
      case identity_regex(expr) => identity(expr)
      case unsupported => throw ClickHouseAnalysisException(s"Unsupported transform expressions: $unsupported")
    }

  def toClickHouse(transform: Transform): String = transform match {
    case YearsTransform(FieldReference(Seq(col))) => s"toYear($col)"
    case MonthsTransform(FieldReference(Seq(col))) => s"toYYYYMM($col)"
    case DaysTransform(FieldReference(Seq(col))) => s"toYYYYMMDD($col)"
    case HoursTransform(FieldReference(Seq(col))) => s"toHour($col)"
    case IdentityTransform(FieldReference(parts)) => parts.mkString(", ")
    case other: Transform => other.describe()
  }
}
