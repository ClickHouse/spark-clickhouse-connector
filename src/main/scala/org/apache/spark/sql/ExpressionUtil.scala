package org.apache.spark.sql

import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.types.DataType

// expose LogicalExpressions
object ExpressionUtil {

  def literal[T](value: T): LiteralValue[T] = LogicalExpressions.literal(value)

  def literal[T](value: T, dataType: DataType): LiteralValue[T] = LogicalExpressions.literal(value, dataType)

  def reference(nameParts: Seq[String]): NamedReference = LogicalExpressions.reference(nameParts)

  def apply(name: String, arguments: Expression*): Transform = LogicalExpressions.apply(name, arguments: _*)

  def bucket(numBuckets: Int, references: Array[NamedReference]): BucketTransform =
    LogicalExpressions.bucket(numBuckets, references)

  def identity(reference: NamedReference): IdentityTransform = LogicalExpressions.identity(reference)

  def years(reference: NamedReference): YearsTransform = LogicalExpressions.years(reference)

  def months(reference: NamedReference): MonthsTransform = LogicalExpressions.months(reference)

  def days(reference: NamedReference): DaysTransform = LogicalExpressions.days(reference)

  def hours(reference: NamedReference): HoursTransform = LogicalExpressions.hours(reference)
}
