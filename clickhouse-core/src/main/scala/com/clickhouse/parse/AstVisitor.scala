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

package com.clickhouse.parse

import com.clickhouse.ClickHouseSQLParser.{ColumnExprContext, ColumnExprFunctionContext, ColumnExprIdentifierContext, ColumnExprListContext, ColumnExprLiteralContext, ColumnExprParensContext, ColumnExprPrecedence1Context, ColumnExprPrecedence2Context, ColumnExprTupleContext, ColumnsExprColumnContext, ColumnsExprContext, EngineClauseContext, OrderByClauseContext, OrderExprContext, SettingsClauseContext}
import com.clickhouse.expr.{Expr, FieldRef, FuncExpr, OrderExpr, StringLiteral, TupleExpr}
import com.clickhouse.spec.{DistributedEngineSpec, MergeTreeEngineSpec, ReplacingMergeTreeEngineSpec, ReplicatedMergeTreeEngineSpec, ReplicatedReplacingMergeTreeEngineSpec, TableEngineSpec, UnknownTableEngineSpec}
import com.clickhouse.{ClickHouseSQLBaseVisitor, Logging, Utils}

import scala.collection.JavaConverters._
import org.antlr.v4.runtime.tree.ParseTree
import com.clickhouse.expr._
import com.clickhouse.spec._
import com.clickhouse.ClickHouseSQLParser._

class AstVisitor extends ClickHouseSQLBaseVisitor[AnyRef] with Logging {
  import ParseUtils._

  protected def typedVisit[T](ctx: ParseTree): T =
    ctx.accept(this).asInstanceOf[T]

  protected def tupleIfNeeded(maybeSingleTupleExpr: List[Expr]): TupleExpr = maybeSingleTupleExpr match {
    case List(tupleExpr: TupleExpr) => tupleExpr
    case exprList: List[Expr] => TupleExpr(exprList)
  }

  override def visitEngineClause(ctx: EngineClauseContext): TableEngineSpec = {
    val engineExpr = source(ctx.engineExpr)
    val engine = source(ctx.engineExpr.identifierOrNull)

    val engineArgs: Seq[Expr] =
      Option(ctx.engineExpr.columnExprList)
        .map(_.columnsExpr.asScala)
        .getOrElse(List.empty)
        .map(visitColumnsExpr)
        .toSeq

    val orderByOpt = listToOption(ctx.orderByClause)
      .map(visitOrderByClause)
      .map(orderByList => tupleIfNeeded(orderByList.map(_.expr)))
    val pkOpt = listToOption(ctx.primaryKeyClause)
      .map(_.columnExpr)
      .map(visitColumnExpr)
    val partOpt = listToOption(ctx.partitionByClause)
      .map(_.columnExpr)
      .map(visitColumnExpr)
    val sampleByOpt = listToOption(ctx.sampleByClause)
      .map(_.columnExpr)
      .map(visitColumnExpr)
    val ttlOpt = listToOption(ctx.ttlClause).map(source) // we don't care about ttl now
    val settings = listToOption(ctx.settingsClause)
      .map(visitSettingsClause)
      .getOrElse(Map.empty)

    engine match {
      case eg: String if "MergeTree" equalsIgnoreCase eg =>
        MergeTreeEngineSpec(
          engine_clause = engineExpr,
          _sorting_key = tupleIfNeeded(orderByOpt.toList),
          _primary_key = tupleIfNeeded(pkOpt.toList),
          _partition_key = tupleIfNeeded(partOpt.toList),
          _sampling_key = tupleIfNeeded(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "ReplacingMergeTree" equalsIgnoreCase eg =>
        ReplacingMergeTreeEngineSpec(
          engine_clause = engineExpr,
          version_column = seqToOption(engineArgs).map(_.asInstanceOf[FieldRef]),
          _sorting_key = tupleIfNeeded(orderByOpt.toList),
          _primary_key = tupleIfNeeded(pkOpt.toList),
          _partition_key = tupleIfNeeded(partOpt.toList),
          _sampling_key = tupleIfNeeded(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "ReplicatedMergeTree" equalsIgnoreCase eg =>
        ReplicatedMergeTreeEngineSpec(
          engine_clause = engineExpr,
          zk_path = engineArgs.head.asInstanceOf[StringLiteral].value,
          replica_name = engineArgs(1).asInstanceOf[StringLiteral].value,
          _sorting_key = tupleIfNeeded(orderByOpt.toList),
          _primary_key = tupleIfNeeded(pkOpt.toList),
          _partition_key = tupleIfNeeded(partOpt.toList),
          _sampling_key = tupleIfNeeded(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "ReplicatedReplacingMergeTree" equalsIgnoreCase eg =>
        ReplicatedReplacingMergeTreeEngineSpec(
          engine_clause = engineExpr,
          zk_path = engineArgs.head.asInstanceOf[StringLiteral].value,
          replica_name = engineArgs(1).asInstanceOf[StringLiteral].value,
          version_column = seqToOption(engineArgs.drop(2)).map(_.asInstanceOf[FieldRef]),
          _sorting_key = tupleIfNeeded(orderByOpt.toList),
          _primary_key = tupleIfNeeded(pkOpt.toList),
          _partition_key = tupleIfNeeded(partOpt.toList),
          _sampling_key = tupleIfNeeded(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "Distributed" equalsIgnoreCase eg =>
        DistributedEngineSpec(
          engine_clause = engineExpr,
          cluster = engineArgs.head.asInstanceOf[StringLiteral].value,
          local_db = engineArgs(1).asInstanceOf[StringLiteral].value,
          local_table = engineArgs(2).asInstanceOf[StringLiteral].value,
          sharding_key = engineArgs.drop(3).headOption,
          _settings = settings
        )
      case _ => UnknownTableEngineSpec(engineExpr)
    }
  }

  // //////////////////////////////////////////////
  // ///////////// visit ColumnExpr ///////////////
  // //////////////////////////////////////////////
  def visitColumnExpr(ctx: ColumnExprContext): Expr = ctx match {
    case fieldCtx: ColumnExprIdentifierContext => visitColumnExprIdentifier(fieldCtx)
    case literalCtx: ColumnExprLiteralContext => visitColumnExprLiteral(literalCtx)
    case funcCtx: ColumnExprFunctionContext => visitColumnExprFunction(funcCtx)
    case singleColCtx: ColumnExprParensContext => visitColumnExpr(singleColCtx.columnExpr)
    case tupleCtx: ColumnExprTupleContext => visitColumnExprTuple(tupleCtx)
    case precedence1Ctx: ColumnExprPrecedence1Context => visitColumnExprPrecedence1(precedence1Ctx)
    case precedence2Ctx: ColumnExprPrecedence2Context => visitColumnExprPrecedence2(precedence2Ctx)
    case other: ColumnExprContext => throw new IllegalArgumentException(
        s"Unsupported ColumnExpr: [${other.getClass.getSimpleName}] ${other.getText}"
      )
  }

  override def visitColumnExprIdentifier(ctx: ColumnExprIdentifierContext): FieldRef =
    FieldRef(source(ctx.columnIdentifier))

  override def visitColumnExprLiteral(ctx: ColumnExprLiteralContext): StringLiteral =
    StringLiteral(Utils.stripSingleQuote(source(ctx.literal)))

  override def visitColumnExprFunction(ctx: ColumnExprFunctionContext): FuncExpr = {
    require(
      ctx.columnExprList == null,
      s"Unsupported ColumnExprFunction with columnExprList: [${ctx.getClass.getSimpleName}] ${ctx.getText}"
    )
    require(
      ctx.DISTINCT == null,
      s"Unsupported ColumnExprFunction with DISTINCT: [${ctx.getClass.getSimpleName}] ${ctx.getText}"
    )

    val funcName = ctx.identifier.getText
    val funArgs = Option(ctx.columnArgList).map(_.columnArgExpr.asScala.toList.map { columnArgExprCtx =>
      require(
        columnArgExprCtx.columnLambdaExpr == null,
        s"Unsupported ColumnLambdaExpr: ${source(columnArgExprCtx)}"
      )
      // recursive visit, but not sure if spark support the nested transform
      visitColumnExpr(columnArgExprCtx.columnExpr)
    }).getOrElse(Nil)
    FuncExpr(funcName, funArgs)
  }

  override def visitColumnExprTuple(ctx: ColumnExprTupleContext): TupleExpr =
    TupleExpr(visitColumnExprList(ctx.columnExprList))

  override def visitColumnExprPrecedence1(ctx: ColumnExprPrecedence1Context): FuncExpr = {
    val funcName =
      if (ctx.PERCENT != null) "remainder"
      else if (ctx.SLASH != null) "divide"
      else if (ctx.ASTERISK != null) "multiply"
      else throw new IllegalArgumentException(s"Invalid [ColumnExprPrecedence1] ${ctx.getText}")
    val funArgs = List(visitColumnExpr(ctx.columnExpr(0)), visitColumnExpr(ctx.columnExpr(1)))
    FuncExpr(funcName, funArgs)
  }

  override def visitColumnExprPrecedence2(ctx: ColumnExprPrecedence2Context): FuncExpr = {
    val funcName =
      if (ctx.PLUS != null) "add"
      else if (ctx.DASH != null) "subtract"
      else if (ctx.CONCAT != null) "concat"
      else throw new IllegalArgumentException(s"Invalid [ColumnExprPrecedence2] ${ctx.getText}")
    val funArgs = List(visitColumnExpr(ctx.columnExpr(0)), visitColumnExpr(ctx.columnExpr(1)))
    FuncExpr(funcName, funArgs)
  }

  // //////////////////////////////////////////////
  // ////////////// visit order by ////////////////
  // //////////////////////////////////////////////

  override def visitOrderByClause(ctx: OrderByClauseContext): List[OrderExpr] =
    ctx.orderExprList.orderExpr.asScala.toList.map(visitOrderExpr)

  override def visitOrderExpr(ctx: OrderExprContext): OrderExpr = {
    val desc = Seq(ctx.DESC, ctx.DESCENDING).exists(_ != null)
    val nullLast = Option(ctx.LAST).nonEmpty
    OrderExpr(visitColumnExpr(ctx.columnExpr()), !desc, !nullLast)
  }

  // //////////////////////////////////////////////
  // /////////////// visit others /////////////////
  // //////////////////////////////////////////////

  override def visitColumnExprList(ctx: ColumnExprListContext): List[Expr] =
    ctx.columnsExpr.asScala.toList.map(visitColumnsExpr)

  def visitColumnsExpr(ctx: ColumnsExprContext): Expr = ctx match {
    case field: ColumnsExprColumnContext => visitColumnExpr(field.columnExpr)
    case other: ColumnsExprContext => throw new IllegalArgumentException(
        s"Unsupported ColumnsExprContext: ${source(other)}"
      )
  }

  override def visitSettingsClause(ctx: SettingsClauseContext): Map[String, String] =
    ctx.settingExprList.settingExpr.asScala.map(se => se.identifier.getText -> source(se.literal)).toMap
}
