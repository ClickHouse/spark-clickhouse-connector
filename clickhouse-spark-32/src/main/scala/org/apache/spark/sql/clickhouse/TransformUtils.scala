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

package org.apache.spark.sql.clickhouse

import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.expressions._
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.expr.{Expr, FieldRef, FuncExpr}

object TransformUtils {
  // Some functions of ClickHouse which match Spark pre-defined Transforms
  //
  // toYear, YEAR - Converts a date or date with time to a UInt16 (AD)
  // toYYYYMM     - Converts a date or date with time to a UInt32 (YYYY*100 + MM)
  // toYYYYMMDD   - Converts a date or date with time to a UInt32 (YYYY*10000 + MM*100 + DD)
  // toHour, HOUR - Converts a         date with time to a UInt8  (0-23)

  def toSparkTransform(expr: Expr): Transform =
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
      case unsupported => throw ClickHouseClientException(s"Unsupported ClickHouse expression: $unsupported")
    }

  // TODO toClickHouseExpr
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
