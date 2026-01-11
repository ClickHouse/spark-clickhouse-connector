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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.IGNORE_UNSUPPORTED_TRANSFORM
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, _}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import com.clickhouse.spark.Logging
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.expr._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object ExprUtils extends SQLConfHelper with Logging {

  def toSparkPartitions(partitionKey: Option[List[Expr]]): Array[Transform] =
    partitionKey.seq.flatten.flatten(toSparkTransformOpt).toArray

  def toSparkSplits(shardingKey: Option[Expr], partitionKey: Option[List[Expr]]): Array[Transform] =
    (shardingKey.seq ++ partitionKey.seq.flatten).flatten(toSparkTransformOpt).toArray

  def toSparkSortOrders(
    shardingKeyIgnoreRand: Option[Expr],
    partitionKey: Option[List[Expr]],
    sortingKey: Option[List[OrderExpr]]
  ): Array[SortOrder] =
    toSparkSplits(shardingKeyIgnoreRand, partitionKey).map(Expressions.sort(_, SortDirection.ASCENDING)) ++:
      sortingKey.seq.flatten.flatten { case OrderExpr(expr, asc, nullFirst) =>
        val direction = if (asc) SortDirection.ASCENDING else SortDirection.DESCENDING
        val nullOrder = if (nullFirst) NullOrdering.NULLS_FIRST else NullOrdering.NULLS_LAST
        toSparkTransformOpt(expr).map(trans => Expressions.sort(trans, direction, nullOrder))
      }.toArray

  @tailrec
  def toCatalyst(v2Expr: V2Expression, fields: Array[StructField]): Expression =
    v2Expr match {
      case IdentityTransform(ref) => toCatalyst(ref, fields)
      case ref: NamedReference if ref.fieldNames.length == 1 =>
        val (field, ordinal) = fields
          .zipWithIndex
          .find { case (field, _) => field.name == ref.fieldNames.head }
          .getOrElse(throw CHClientException(s"Invalid field reference: $ref"))
        BoundReference(ordinal, field.dataType, field.nullable)
      case _ => throw CHClientException(
          s"Unsupported V2 expression: $v2Expr, SPARK-33779: Spark 3.3 only support IdentityTransform"
        )
    }

  def toSparkTransformOpt(expr: Expr): Option[Transform] = expr match {
    case FuncExpr("tuple", Nil) => None // tuple() means no partitioning
    case _ => Try(toSparkTransform(expr)) match {
      case Success(t) => Some(t)
      case Failure(cause) if conf.getConf(IGNORE_UNSUPPORTED_TRANSFORM) =>
        log.warn(s"Ignoring unsupported ClickHouse partition/sharding expression: $expr. " +
          s"Spark-side repartitioning will be skipped for this expression. " +
          s"To fail on unsupported expressions, set ${IGNORE_UNSUPPORTED_TRANSFORM.key}=false. " +
          s"Reason: ${cause.getMessage}")
        None
      case Failure(rethrow) => throw new AnalysisException(rethrow.getMessage, cause = Some(rethrow))
    }
  }

  // Some functions of ClickHouse which match Spark pre-defined Transforms
  //
  // toYear, YEAR - Converts a date or date with time to a UInt16 (AD)
  // toYYYYMM     - Converts a date or date with time to a UInt32 (YYYY*100 + MM)
  // toYYYYMMDD   - Converts a date or date with time to a UInt32 (YYYY*10000 + MM*100 + DD)
  // toHour, HOUR - Converts a         date with time to a UInt8  (0-23)

  def toSparkTransform(expr: Expr): Transform = expr match {
    case FieldRef(col) => identity(col)
    case FuncExpr("toYear", List(FieldRef(col))) => years(col)
    case FuncExpr("YEAR", List(FieldRef(col))) => years(col)
    case FuncExpr("toYYYYMM", List(FieldRef(col))) => months(col)
    case FuncExpr("toYYYYMMDD", List(FieldRef(col))) => days(col)
    case FuncExpr("toHour", List(FieldRef(col))) => hours(col)
    case FuncExpr("HOUR", List(FieldRef(col))) => hours(col)
    // TODO support arbitrary functions
    // case FuncExpr("xxHash64", List(FieldRef(col))) => apply("ck_xx_hash64", column(col))
    case FuncExpr("rand", Nil) => apply("rand")
    case FuncExpr("toYYYYMMDD", List(FuncExpr("toDate", List(FieldRef(col))))) => identity(col)
    case unsupported => throw CHClientException(s"Unsupported ClickHouse expression: $unsupported")
  }

  def toClickHouse(transform: Transform): Expr = transform match {
    case YearsTransform(FieldReference(Seq(col))) => FuncExpr("toYear", List(FieldRef(col)))
    case MonthsTransform(FieldReference(Seq(col))) => FuncExpr("toYYYYMM", List(FieldRef(col)))
    case DaysTransform(FieldReference(Seq(col))) => FuncExpr("toYYYYMMDD", List(FieldRef(col)))
    case HoursTransform(FieldReference(Seq(col))) => FuncExpr("toHour", List(FieldRef(col)))
    case IdentityTransform(fieldRefs) => FieldRef(fieldRefs.describe)
    case ApplyTransform(name, args) => FuncExpr(name, args.map(arg => SQLExpr(arg.describe())).toList)
    case bucket: BucketTransform => throw CHClientException(s"Bucket transform not support yet: $bucket")
    case other: Transform => throw CHClientException(s"Unsupported transform: $other")
  }

  def inferTransformSchema(
    primarySchema: StructType,
    secondarySchema: StructType,
    transform: Transform
  ): StructField = transform match {
    case years: YearsTransform => StructField(years.toString, IntegerType)
    case months: MonthsTransform => StructField(months.toString, IntegerType)
    case days: DaysTransform => StructField(days.toString, IntegerType)
    case hours: HoursTransform => StructField(hours.toString, IntegerType)
    case IdentityTransform(FieldReference(Seq(col))) => primarySchema.find(_.name == col)
        .orElse(secondarySchema.find(_.name == col))
        .getOrElse(throw CHClientException(s"Invalid partition column: $col"))
    case ckXxhHash64 @ ApplyTransform("ck_xx_hash64", _) => StructField(ckXxhHash64.toString, LongType)
    case bucket: BucketTransform => throw CHClientException(s"Bucket transform not support yet: $bucket")
    case other: Transform => throw CHClientException(s"Unsupported transform: $other")
  }
}
