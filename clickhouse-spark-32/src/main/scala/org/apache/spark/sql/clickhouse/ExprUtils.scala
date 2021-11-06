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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.clickhouse.TransformUtils.toSparkTransform
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, _}
import org.apache.spark.sql.types.StructField
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.expr.{Expr, OrderExpr}

object ExprUtils {

  def toSparkParts(shardingKey: Option[Expr], partitionKey: Option[List[Expr]]): Array[Transform] =
    (shardingKey.seq ++ partitionKey.seq.flatten).map(toSparkTransform).toArray

  def toSparkSortOrders(sortingKey: Option[List[OrderExpr]]): Array[SortOrder] =
    sortingKey.seq.flatten.map { case OrderExpr(expr, asc, nullFirst) =>
      val direction = if (asc) SortDirection.ASCENDING else SortDirection.DESCENDING
      val nullOrder = if (nullFirst) NullOrdering.NULLS_FIRST else NullOrdering.NULLS_LAST
      Expressions.sort(toSparkTransform(expr), direction, nullOrder)
    }.toArray

  @tailrec
  def toCatalyst(v2Expr: V2Expression, fields: Array[StructField]): Expression =
    v2Expr match {
      case IdentityTransform(ref) => toCatalyst(ref, fields)
      case ref: FieldReference if ref.fieldNames.length == 1 =>
        val (field, ordinal) = fields
          .zipWithIndex
          .find { case (field, _) => field.name == ref.fieldNames.head }
          .getOrElse(throw ClickHouseClientException(s"Invalid FieldReference: $ref"))
        BoundReference(ordinal, field.dataType, field.nullable)
      case _ => throw ClickHouseClientException(
          s"Unsupported V2 expression: $v2Expr, SPARK-33779: Spark 3.2 only support IdentityTransform"
        )
    }
}
