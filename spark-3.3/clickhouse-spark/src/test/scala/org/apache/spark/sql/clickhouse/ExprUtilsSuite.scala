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

import com.clickhouse.spark.expr.{FieldRef, FuncExpr, OrderExpr, SQLExpr}
import com.clickhouse.spark.read.ClickHouseScanBuilder
import org.apache.spark.sql.connector.expressions.{
  Expressions,
  GeneralScalarExpression,
  LiteralValue,
  NullOrdering,
  SortDirection
}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

class ExprUtilsSuite extends AnyFunSuite {

  test("toClickHouseSortOrderOpt: ASC NULLS FIRST on bare field") {
    val sort = Expressions.sort(
      Expressions.column("id"),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_FIRST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort) ===
      Some(OrderExpr(FieldRef("id"), asc = true, nullFirst = true)))
  }

  test("toClickHouseSortOrderOpt: DESC NULLS LAST on bare field") {
    val sort = Expressions.sort(
      Expressions.column("ts"),
      SortDirection.DESCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort) ===
      Some(OrderExpr(FieldRef("ts"), asc = false, nullFirst = false)))
  }

  test("toClickHouseSortOrderOpt: identity transform on field") {
    val sort = Expressions.sort(
      Expressions.identity("k"),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort) ===
      Some(OrderExpr(FieldRef("k"), asc = true, nullFirst = false)))
  }

  test("toClickHouseSortOrderOpt: ApplyTransform outside hardcoded allow-list returns None (3.3)") {
    // 3.3 has no FunctionRegistry, so the sort-pushdown path refuses any
    // ApplyTransform whose name isn't one of the four hardcoded mappings --
    // forwarding an arbitrary name would risk emitting SQL the server rejects.
    val sort = Expressions.sort(
      Expressions.apply("xxHash64", Expressions.column("k")),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort).isEmpty)
  }

  test("renderOrderExpr: bare field name that is a CH reserved word is back-quoted") {
    val rendered = ClickHouseScanBuilder.renderOrderExpr(
      OrderExpr(FieldRef("order"), asc = true, nullFirst = false)
    )
    assert(rendered === "`order` ASC NULLS LAST")
  }

  test("renderOrderExpr: function-transform leaf field is back-quoted") {
    val rendered = ClickHouseScanBuilder.renderOrderExpr(
      OrderExpr(FuncExpr("xxHash64", List(FieldRef("order"))), asc = true, nullFirst = false)
    )
    assert(rendered === "xxHash64(`order`) ASC NULLS LAST")
  }

  test("renderOrderExpr: SQLExpr leaf is emitted verbatim (e.g. literal arg)") {
    val rendered = ClickHouseScanBuilder.renderOrderExpr(
      OrderExpr(FuncExpr("toStartOfInterval", List(FieldRef("ts"), SQLExpr("INTERVAL 1 HOUR"))), asc = false)
    )
    assert(rendered === "toStartOfInterval(`ts`,INTERVAL 1 HOUR) DESC NULLS LAST")
  }

  test("toClickHouseSortOrderOpt + renderOrderExpr: end-to-end with reserved-word column under IdentityTransform") {
    val sort = Expressions.sort(
      Expressions.identity("order"),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    val translated = ExprUtils.toClickHouseSortOrderOpt(sort)
    assert(translated.isDefined)
    assert(ClickHouseScanBuilder.renderOrderExpr(translated.get) === "`order` ASC NULLS LAST")
  }

  test("toClickHouseSortOrderOpt: literal expression returns None") {
    val sort = Expressions.sort(
      LiteralValue[Integer](Integer.valueOf(1), IntegerType),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort).isEmpty)
  }

  test("toClickHouseSortOrderOpt: arbitrary GeneralScalarExpression returns None") {
    val sort = Expressions.sort(
      new GeneralScalarExpression("=", Array(Expressions.column("k"), LiteralValue(1, IntegerType))),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort).isEmpty)
  }
}
