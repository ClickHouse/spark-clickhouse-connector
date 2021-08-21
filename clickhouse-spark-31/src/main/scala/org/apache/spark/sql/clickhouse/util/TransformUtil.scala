package org.apache.spark.sql.clickhouse.util

import scala.language.postfixOps

import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import xenon.clickhouse.exception.ClickHouseClientException

import xenon.clickhouse.expr._

object TransformUtil {
  // Some functions of ClickHouse which match Spark pre-defined Transforms
  //
  // toYear     - Converts a date or date with time to a UInt16 (AD)                       Alias: YEAR
  // toYYYYMM   - Converts a date or date with time to a UInt32 (YYYY*100 + MM)
  // toYYYYMMDD - Converts a date or date with time to a UInt32 (YYYY*10000 + MM*100 + DD)
  // toHour     - Converts a date with time to a         UInt8  (0-23)                     Alias: HOUR

  def fromClickHouse(expr: Expr): Transform = {
    expr match {
      case FieldRef(col) => identity(col)
      case FuncExpr("toYear", List(FieldRef(col))) => years(col)
      case FuncExpr("YEAR", List(FieldRef(col))) => years(col)
      case FuncExpr("toYYYYMM", List(FieldRef(col))) => months(col)
      case FuncExpr("toYYYYMMDD", List(FieldRef(col))) => days(col)
      case FuncExpr("toHour", List(FieldRef(col))) => hours(col)
      case FuncExpr("HOUR", List(FieldRef(col))) => hours(col)
      // TODO support arbitrary functions
      case FuncExpr("xxHash64", List(FieldRef(col))) => apply("ck_xx_hash64", column(col))
      case unsupported => throw ClickHouseClientException(s"Unsupported transform expression: $unsupported")
    }
  }

  def toClickHouse(transform: Transform): String = transform match {
    case YearsTransform(FieldReference(Seq(col))) => s"toYear($col)"
    case MonthsTransform(FieldReference(Seq(col))) => s"toYYYYMM($col)"
    case DaysTransform(FieldReference(Seq(col))) => s"toYYYYMMDD($col)"
    case HoursTransform(FieldReference(Seq(col))) => s"toHour($col)"
    case IdentityTransform(FieldReference(parts)) => parts.mkString(", ")
    case function: ApplyTransform => function.describe()
    case bucket: BucketTransform => throw ClickHouseClientException(s"Bucket transform not support yet: $bucket")
    case other: Transform => throw ClickHouseClientException(s"Unsupported transform: $other")
  }
}
