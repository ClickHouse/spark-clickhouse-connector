package org.apache.spark.sql.clickhouse.util

import scala.util.matching.Regex

import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.Expressions._
import xenon.clickhouse.exception.ClickHouseClientException

abstract class TransformWrapper extends Transform {

  def delegate: Transform

  override def name(): String = s"clickhouse_shard(${delegate.name})"

  override def references(): Array[NamedReference] = delegate.references

  override def arguments(): Array[Expression] = delegate.arguments

  override def describe(): String = delegate.describe
}

case class ClickHouseShardTransform(override val delegate: Transform) extends TransformWrapper {
  override def name(): String = s"clickhouse_shard__${delegate.name}"
}

case class ClickHousePartitionTransform(override val delegate: Transform) extends TransformWrapper {
  override def name(): String = s"clickhouse_partition__${delegate.name}"
}

object TransformUtil {
  // Some functions of ClickHouse which match Spark pre-defined Transforms
  //
  // toYear     - Converts a date or date with time to a UInt16 (AD)                       Alias: YEAR
  // toYYYYMM   - Converts a date or date with time to a UInt32 (YYYY*100 + MM)
  // toYYYYMMDD - Converts a date or date with time to a UInt32 (YYYY*10000 + MM*100 + DD)
  // toHour     - Converts a date with time to a         UInt8  (0-23)                     Alias: HOUR

  // format: off
  private[sql] val years_transform_regex:  Regex = """(toYear|YEAR)\((\w+)\)""".r
  private[sql] val months_transform_regex: Regex = """toYYYYMM\((\w+)\)""".r
  private[sql] val days_transform_regex:   Regex = """toYYYYMMDD\((\w+)\)""".r
  private[sql] val hours_transform_regex:  Regex = """(toHour|HOUR)\((\w+)\)""".r
  private[sql] val identity_regex:         Regex = """^(\w+)$""".r
  private[sql] val xx_hash_64_regex:       Regex = """^xxHash64\((.+)\)$"""r
  // format: on

  def fromClickHouse(transformExpr: String): Transform =
    transformExpr match {
      case years_transform_regex(_, expr) => years(expr)
      case months_transform_regex(expr) => months(expr)
      case days_transform_regex(expr) => days(expr)
      case hours_transform_regex(_, expr) => hours(expr)
      // Assume all others is just a column name without any transforms,
      // thus, `xxHash64(abc)` is not supported yet.
      case identity_regex(expr) => identity(expr)
      case xx_hash_64_regex(expr) => apply("ck_xx_hash64", column(expr))
      // TODO support arbitrary functions
      case unsupported => throw ClickHouseClientException(s"Unsupported transform expression: $unsupported")
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

  def wrapShard(transform: Transform): ClickHouseShardTransform = ClickHouseShardTransform(transform)

  def wrapPartition(transform: Transform): ClickHousePartitionTransform = ClickHousePartitionTransform(transform)
}
