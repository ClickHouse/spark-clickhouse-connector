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
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, TransformExpression}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.IGNORE_UNSUPPORTED_TRANSFORM
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, _}
import org.apache.spark.sql.types.{StructField, StructType}
import xenon.clickhouse.exception.CHClientException
import xenon.clickhouse.expr._
import xenon.clickhouse.func.FunctionRegistry
import xenon.clickhouse.spec.ClusterSpec

import scala.util.{Failure, Success, Try}

object ExprUtils extends SQLConfHelper with Serializable {

  def toSparkPartitions(
    partitionKey: Option[List[Expr]],
    functionRegistry: FunctionRegistry
  ): Array[Transform] =
    partitionKey.seq.flatten.flatten(toSparkTransformOpt(_, functionRegistry)).toArray

  def toSparkSplits(
    shardingKey: Option[Expr],
    partitionKey: Option[List[Expr]],
    functionRegistry: FunctionRegistry
  ): Array[Transform] =
    (shardingKey.seq ++ partitionKey.seq.flatten).flatten(toSparkTransformOpt(_, functionRegistry)).toArray

  def toSparkSortOrders(
    shardingKeyIgnoreRand: Option[Expr],
    partitionKey: Option[List[Expr]],
    sortingKey: Option[List[OrderExpr]],
    cluster: Option[ClusterSpec],
    functionRegistry: FunctionRegistry
  ): Array[SortOrder] =
    toSparkSplits(
      shardingKeyIgnoreRand.map(k => ExprUtils.toSplitWithModulo(k, cluster.get.totalWeight)),
      partitionKey,
      functionRegistry
    ).map(Expressions.sort(_, SortDirection.ASCENDING)) ++:
      sortingKey.seq.flatten.flatten { case OrderExpr(expr, asc, nullFirst) =>
        val direction = if (asc) SortDirection.ASCENDING else SortDirection.DESCENDING
        val nullOrder = if (nullFirst) NullOrdering.NULLS_FIRST else NullOrdering.NULLS_LAST
        toSparkTransformOpt(expr, functionRegistry).map(trans =>
          Expressions.sort(trans, direction, nullOrder)
        )
      }.toArray

  private def loadV2FunctionOpt(
    name: String,
    args: Seq[Expression],
    functionRegistry: FunctionRegistry
  ): Option[BoundFunction] = {
    def loadFunction(ident: Identifier): UnboundFunction =
      functionRegistry.load(ident.name).getOrElse(throw new NoSuchFunctionException(ident))
    val inputType = StructType(args.zipWithIndex.map {
      case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
    })
    try {
      val unbound = loadFunction(Identifier.of(Array.empty, name))
      Some(unbound.bind(inputType))
    } catch {
      case e: NoSuchFunctionException =>
        throw e
      case _: UnsupportedOperationException if conf.getConf(IGNORE_UNSUPPORTED_TRANSFORM) =>
        None
      case e: UnsupportedOperationException =>
        throw new AnalysisException(e.getMessage, cause = Some(e))
    }
  }

  def toCatalyst(
    v2Expr: V2Expression,
    fields: Array[StructField],
    functionRegistry: FunctionRegistry
  ): Expression =
    v2Expr match {
      case IdentityTransform(ref) => toCatalyst(ref, fields, functionRegistry)
      case ref: NamedReference if ref.fieldNames.length == 1 =>
        val (field, ordinal) = fields
          .zipWithIndex
          .find { case (field, _) => field.name == ref.fieldNames.head }
          .getOrElse(throw CHClientException(s"Invalid field reference: $ref"))
        BoundReference(ordinal, field.dataType, field.nullable)
      case t: Transform =>
        val catalystArgs = t.arguments().map(toCatalyst(_, fields, functionRegistry))
        loadV2FunctionOpt(t.name(), catalystArgs, functionRegistry)
          .map(bound => TransformExpression(bound, catalystArgs)).getOrElse {
            throw CHClientException(s"Unsupported expression: $v2Expr")
          }
      case _ => throw CHClientException(
          s"Unsupported expression: $v2Expr"
        )
    }

  def toSparkTransformOpt(expr: Expr, functionRegistry: FunctionRegistry): Option[Transform] =
    Try(toSparkExpression(expr, functionRegistry)) match {
      // need this function because spark `Table`'s `partitioning` field should be `Transform`
      case Success(t: Transform) => Some(t)
      case Success(_) => None
      case Failure(_) if conf.getConf(IGNORE_UNSUPPORTED_TRANSFORM) => None
      case Failure(rethrow) => throw new AnalysisException(rethrow.getMessage, cause = Some(rethrow))
    }

  def toSparkExpression(expr: Expr, functionRegistry: FunctionRegistry): V2Expression =
    expr match {
      case FieldRef(col) => identity(col)
      case StringLiteral(value) => literal(value)
      case FuncExpr("rand", Nil) => apply("rand")
      case FuncExpr("toYYYYMMDD", List(FuncExpr("toDate", List(FieldRef(col))))) => identity(col)
      case FuncExpr(funName, args) if functionRegistry.getFuncMappingByCk.contains(funName) =>
        apply(functionRegistry.getFuncMappingByCk(funName), args.map(toSparkExpression(_, functionRegistry)): _*)
      case unsupported => throw CHClientException(s"Unsupported ClickHouse expression: $unsupported")
    }

  def toClickHouse(
    transform: Transform,
    functionRegistry: FunctionRegistry
  ): Expr = transform match {
    case IdentityTransform(fieldRefs) => FieldRef(fieldRefs.describe)
    case ApplyTransform(name, args) if functionRegistry.getFuncMappingBySpark.contains(name) =>
      FuncExpr(functionRegistry.getFuncMappingBySpark(name), args.map(arg => SQLExpr(arg.describe())).toList)
    case bucket: BucketTransform => throw CHClientException(s"Bucket transform not support yet: $bucket")
    case other: Transform => throw CHClientException(s"Unsupported transform: $other")
  }

  def inferTransformSchema(
    primarySchema: StructType,
    secondarySchema: StructType,
    transform: Transform,
    functionRegistry: FunctionRegistry
  ): StructField = transform match {
    case IdentityTransform(FieldReference(Seq(col))) => primarySchema.find(_.name == col)
        .orElse(secondarySchema.find(_.name == col))
        .getOrElse(throw CHClientException(s"Invalid partition column: $col"))
    case t @ ApplyTransform(transformName, _) if functionRegistry.load(transformName).isDefined =>
      val resType =
        functionRegistry.load(transformName).getOrElse(throw new NoSuchFunctionException(transformName)) match {
          case f: ScalarFunction[_] => f.resultType()
          case other => throw CHClientException(s"Unsupported function: $other")
        }
      StructField(t.toString, resType)
    case bucket: BucketTransform => throw CHClientException(s"Bucket transform not support yet: $bucket")
    case other: Transform => throw CHClientException(s"Unsupported transform: $other")
  }

  def toSplitWithModulo(shardingKey: Expr, weight: Int): FuncExpr =
    FuncExpr("positiveModulo", List(shardingKey, StringLiteral(weight.toString)))
}
