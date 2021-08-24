package xenon.clickhouse

import org.apache.spark.sql.clickhouse.TransformUtils.toSparkTransform
import org.apache.spark.sql.connector.expressions.Expressions._
import org.apache.spark.sql.connector.expressions.{NullOrdering, SortDirection, SortOrder, Transform}
import xenon.clickhouse.expr.{Expr, OrderExpr}

object ExprUtils {

  def toSparkParts(shardingKey: Option[Expr], partitionKey: Option[List[Expr]]): Array[Transform] =
    (shardingKey.seq ++ partitionKey.seq.flatten).map(toSparkTransform).toArray

  def toSparkSortOrder(sortingKey: Option[List[OrderExpr]]): Array[SortOrder] =
    sortingKey.seq.flatten.map { case OrderExpr(expr, asc, nullFirst) =>
      val direction = if (asc) SortDirection.ASCENDING else SortDirection.DESCENDING
      val nullOrder = if (nullFirst) NullOrdering.NULLS_FIRST else NullOrdering.NULLS_LAST
      sort(toSparkTransform(expr), direction, nullOrder)
    }.toArray
}
