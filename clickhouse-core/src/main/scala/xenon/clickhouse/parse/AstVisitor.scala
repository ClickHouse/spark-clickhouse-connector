package xenon.clickhouse.parse

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.tree.ParseTree
import xenon.clickhouse.{ClickHouseAstBaseVisitor, Logging, Utils}
import xenon.clickhouse.spec._
import xenon.clickhouse.ClickHouseAstParser._
import xenon.clickhouse.expr._

class AstVisitor extends ClickHouseAstBaseVisitor[AnyRef] with Logging {
  import ParseUtil._

  protected def typedVisit[T](ctx: ParseTree): T =
    ctx.accept(this).asInstanceOf[T]

  override def visitEngineClause(ctx: EngineClauseContext): TableEngineSpecV2 = {
    val engineExpr = source(ctx.engineExpr)
    val engine = source(ctx.engineExpr.identifierOrNull)

    val engineArgs: Seq[Expr] =
      Option(ctx.engineExpr.columnExprList)
        .map(_.columnsExpr.asScala)
        .getOrElse(List.empty)
        .map(visitColumnsExpr)

    val orderByOpt = listToOption(ctx.orderByClause).map(visitOrderByClause)
    val pkOpt = listToOption(ctx.primaryKeyClause).map(_.columnExpr).map(visitColumnExpr)
    val partOpt = listToOption(ctx.partitionByClause).map(_.columnExpr).map(visitColumnExpr)
    val sampleByOpt = listToOption(ctx.sampleByClause).map(_.columnExpr).map(visitColumnExpr)
    // we don't care about ttl now
    val ttlOpt = listToOption(ctx.ttlClause).map(source)
    val settings = listToOption(ctx.settingsClause).map(visitSettingsClause).getOrElse(Map.empty)

    engine match {
      case eg: String if "MergeTree" equalsIgnoreCase eg =>
        MergeTreeEngineSpecV2(
          engine_expr = engineExpr,
          _sorting_key = orderByOpt.getOrElse(List.empty),
          _primary_key = TupleExpr(pkOpt.toList),
          _partition_key = TupleExpr(partOpt.toList),
          _sampling_key = TupleExpr(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "ReplacingMergeTree" equalsIgnoreCase eg =>
        ReplacingMergeTreeEngineSpecV2(
          engine_expr = engineExpr,
          version_column = seqToOption(engineArgs).map(_.asInstanceOf[FieldRef]),
          _sorting_key = orderByOpt.getOrElse(List.empty),
          _primary_key = TupleExpr(pkOpt.toList),
          _partition_key = TupleExpr(partOpt.toList),
          _sampling_key = TupleExpr(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "ReplicatedMergeTree" equalsIgnoreCase eg =>
        ReplicatedMergeTreeEngineSpecV2(
          engine_expr = engineExpr,
          zk_path = engineArgs.head.sql,
          replica_name = engineArgs(1).sql,
          _sorting_key = orderByOpt.getOrElse(List.empty),
          _primary_key = TupleExpr(pkOpt.toList),
          _partition_key = TupleExpr(partOpt.toList),
          _sampling_key = TupleExpr(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "ReplicatedReplacingMergeTree" equalsIgnoreCase eg =>
        ReplicatedReplacingMergeTreeEngineSpecV2(
          engine_expr = engineExpr,
          zk_path = engineArgs.head.sql,
          replica_name = engineArgs(1).sql,
          version_column = seqToOption(engineArgs.drop(2)).map(_.asInstanceOf[FieldRef]),
          _sorting_key = orderByOpt.getOrElse(List.empty),
          _primary_key = TupleExpr(pkOpt.toList),
          _partition_key = TupleExpr(partOpt.toList),
          _sampling_key = TupleExpr(sampleByOpt.toList),
          _ttl = ttlOpt,
          _settings = settings
        )
      case eg: String if "Distributed" equalsIgnoreCase eg =>
        DistributedEngineSpecV2(
          engine_expr = engineExpr,
          cluster = engineArgs.head.sql,
          local_db = engineArgs(1).sql,
          local_table = engineArgs(2).sql,
          sharding_key = engineArgs.drop(3).headOption,
          _settings = settings
        )
      case _ => UnknownTableEngineSpecV2(engineExpr)
    }
  }

  ////////////////////////////////////////////////
  /////////////// visit ColumnExpr ///////////////
  ////////////////////////////////////////////////
  def visitColumnExpr(ctx: ColumnExprContext): Expr = ctx match {
    case fieldCtx: ColumnExprIdentifierContext => visitColumnExprIdentifier(fieldCtx)
    case literalCtx: ColumnExprLiteralContext => visitColumnExprLiteral(literalCtx)
    case funcCtx: ColumnExprFunctionContext => visitColumnExprFunction(funcCtx)
    case other: ColumnExprContext => throw new IllegalArgumentException(
        s"Unsupported ColumnExpr: [${other.getClass.getSimpleName}] ${other.getText}"
      )
  }

  override def visitColumnExprIdentifier(ctx: ColumnExprIdentifierContext): FieldRef =
    FieldRef(source(ctx.columnIdentifier))

  override def visitColumnExprLiteral(ctx: ColumnExprLiteralContext): StringLiteral =
    StringLiteral(Utils.stripSingleQuote(source(ctx.literal)))

  override def visitColumnExprFunction(ctx: ColumnExprFunctionContext): FuncExpr = {
    if (ctx.columnExprList != null) throw new IllegalArgumentException(
      s"Unsupported ColumnExprFunction with columnExprList: [${ctx.getClass.getSimpleName}] ${ctx.getText}"
    )
    if (ctx.DISTINCT != null) throw new IllegalArgumentException(
      s"Unsupported ColumnExprFunction with DISTINCT: [${ctx.getClass.getSimpleName}] ${ctx.getText}"
    )

    val funcName = ctx.identifier.getText
    val funArgs = ctx.columnArgList.columnArgExpr.asScala.toList.map { columnArgExprCtx =>
      if (columnArgExprCtx.columnLambdaExpr != null) throw new IllegalArgumentException(
        s"Unsupported ColumnLambdaExpr: ${source(columnArgExprCtx)}"
      )
      // recursive visit, but not sure if spark support the nested transform
      visitColumnExpr(columnArgExprCtx.columnExpr)
    }
    FuncExpr(funcName, funArgs)
  }

  ////////////////////////////////////////////////
  //////////////// visit order by ////////////////
  ////////////////////////////////////////////////

  override def visitOrderByClause(ctx: OrderByClauseContext): List[OrderExpr] =
    ctx.orderExprList.orderExpr.asScala.toList.map(visitOrderExpr)

  override def visitOrderExpr(ctx: OrderExprContext): OrderExpr = {
    val desc = Seq(ctx.DESC, ctx.DESCENDING).exists(_ != null)
    val nullLast = Option(ctx.LAST).nonEmpty
    OrderExpr(visitColumnExpr(ctx.columnExpr()), !desc, !nullLast)
  }

  ////////////////////////////////////////////////
  ///////////////// visit others /////////////////
  ////////////////////////////////////////////////

  def visitColumnsExpr(ctx: ColumnsExprContext): Expr = ctx match {
    case field: ColumnsExprColumnContext => visitColumnExpr(field.columnExpr)
    case other: ColumnsExprContext => throw new IllegalArgumentException(
        s"unsupported ColumnsExprContext: ${source(other)}"
      )
  }

  override def visitSettingsClause(ctx: SettingsClauseContext): Map[String, String] =
    ctx.settingExprList.settingExpr.asScala.map(se => se.identifier.getText -> source(se.literal)).toMap
}
