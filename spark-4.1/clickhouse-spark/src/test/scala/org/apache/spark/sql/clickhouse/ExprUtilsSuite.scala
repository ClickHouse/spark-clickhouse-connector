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
import com.clickhouse.spark.func.{
  ClickHouseShardNum,
  ClickHouseXxHash64,
  DynamicFunctionRegistry,
  StaticFunctionRegistry
}
import com.clickhouse.spark.expr.ExprRender
import com.clickhouse.spark.spec.{ClusterSpec, NodeSpec, ReplicaSpec, ShardSpec}
import org.apache.spark.sql.connector.expressions.{
  ApplyTransform,
  Expressions,
  FieldReference,
  GeneralScalarExpression,
  IdentityTransform,
  LiteralValue,
  NullOrdering,
  SortDirection,
  SortOrder
}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

class ExprUtilsSuite extends AnyFunSuite {

  private val registry = {
    val r = new DynamicFunctionRegistry
    r.register("ck_xx_hash64", ClickHouseXxHash64)
    r
  }

  private val clusterSpec = ClusterSpec(
    "test_cluster",
    Array(
      ShardSpec(1, 1, Array(ReplicaSpec(1, NodeSpec("s1r1")))),
      ShardSpec(2, 1, Array(ReplicaSpec(1, NodeSpec("s2r1"))))
    )
  )

  private def assertPartitionAndSortingSegments(orders: Array[SortOrder]): Unit = {
    assert(orders(1).direction() === SortDirection.ASCENDING)
    orders(1).expression() match {
      case IdentityTransform(FieldReference(Seq("m"))) =>
      case other => fail(s"Unexpected partition sort expression: $other")
    }
    assert(orders(2).direction() === SortDirection.ASCENDING)
    assert(orders(2).nullOrdering() === NullOrdering.NULLS_LAST)
    orders(2).expression() match {
      case IdentityTransform(FieldReference(Seq("id"))) =>
      case other => fail(s"Unexpected sorting key sort expression: $other")
    }
  }

  test("toSparkSortOrders: sharding key sorts by shard num when enabled and cluster is given") {
    val orders = ExprUtils.toSparkSortOrders(
      shardingKeyIgnoreRand = Some(FieldRef("y")),
      partitionKey = Some(List(FieldRef("m"))),
      sortingKey = Some(List(OrderExpr(FieldRef("id"), asc = true, nullFirst = false))),
      cluster = Some(clusterSpec),
      sortByShardNum = true,
      functionRegistry = StaticFunctionRegistry
    )
    assert(orders.length === 3)
    assert(orders(0).direction() === SortDirection.ASCENDING)
    orders(0).expression() match {
      case ApplyTransform(name, Seq(lit: LiteralValue[_], IdentityTransform(FieldReference(Seq("y"))))) =>
        assert(name === ClickHouseShardNum.funcName)
        assert(lit.value === "test_cluster")
      case other => fail(s"Unexpected sharding sort expression: $other")
    }
    assertPartitionAndSortingSegments(orders)
  }

  test("toSparkSortOrders: sharding key sorts by raw key when cluster is absent") {
    val orders = ExprUtils.toSparkSortOrders(
      shardingKeyIgnoreRand = Some(FieldRef("y")),
      partitionKey = Some(List(FieldRef("m"))),
      sortingKey = Some(List(OrderExpr(FieldRef("id"), asc = true, nullFirst = false))),
      cluster = None,
      sortByShardNum = true,
      functionRegistry = StaticFunctionRegistry
    )
    assert(orders.length === 3)
    assert(orders(0).direction() === SortDirection.ASCENDING)
    orders(0).expression() match {
      case IdentityTransform(FieldReference(Seq("y"))) =>
      case other => fail(s"Unexpected sharding sort expression: $other")
    }
    assertPartitionAndSortingSegments(orders)
  }

  test("toSparkSortOrders: sharding key sorts by raw key when sortByShardNum is disabled") {
    val orders = ExprUtils.toSparkSortOrders(
      shardingKeyIgnoreRand = Some(FieldRef("y")),
      partitionKey = Some(List(FieldRef("m"))),
      sortingKey = Some(List(OrderExpr(FieldRef("id"), asc = true, nullFirst = false))),
      cluster = Some(clusterSpec),
      sortByShardNum = false,
      functionRegistry = StaticFunctionRegistry
    )
    assert(orders.length === 3)
    orders(0).expression() match {
      case IdentityTransform(FieldReference(Seq("y"))) =>
      case other => fail(s"Unexpected sharding sort expression: $other")
    }
    assertPartitionAndSortingSegments(orders)
  }

  test("toSparkSortOrders: hash function sharding key is wrapped in shard num") {
    val sparkHashName = StaticFunctionRegistry.clickHouseToSparkFunc("xxHash64")
    val orders = ExprUtils.toSparkSortOrders(
      shardingKeyIgnoreRand = Some(FuncExpr("xxHash64", List(FieldRef("s")))),
      partitionKey = None,
      sortingKey = None,
      cluster = Some(clusterSpec),
      sortByShardNum = true,
      functionRegistry = StaticFunctionRegistry
    )
    assert(orders.length === 1)
    orders(0).expression() match {
      case ApplyTransform(
            name,
            Seq(lit: LiteralValue[_], ApplyTransform(hashName, Seq(IdentityTransform(FieldReference(Seq("s"))))))
          ) =>
        assert(name === ClickHouseShardNum.funcName)
        assert(lit.value === "test_cluster")
        assert(hashName === sparkHashName)
      case other => fail(s"Unexpected sharding sort expression: $other")
    }
  }

  test("toClickHouseSortOrderOpt: ASC NULLS FIRST on bare field") {
    val sort = Expressions.sort(
      Expressions.column("id"),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_FIRST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, StaticFunctionRegistry) ===
      Some(OrderExpr(FieldRef("id"), asc = true, nullFirst = true)))
  }

  test("toClickHouseSortOrderOpt: DESC NULLS LAST on bare field") {
    val sort = Expressions.sort(
      Expressions.column("ts"),
      SortDirection.DESCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, StaticFunctionRegistry) ===
      Some(OrderExpr(FieldRef("ts"), asc = false, nullFirst = false)))
  }

  test("toClickHouseSortOrderOpt: identity transform on field") {
    val sort = Expressions.sort(
      Expressions.identity("k"),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, StaticFunctionRegistry) ===
      Some(OrderExpr(FieldRef("k"), asc = true, nullFirst = false)))
  }

  test("toClickHouseSortOrderOpt: function transform registered in registry") {
    val sort = Expressions.sort(
      Expressions.apply("ck_xx_hash64", Expressions.column("k")),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    val translated = ExprUtils.toClickHouseSortOrderOpt(sort, registry)
    assert(translated === Some(
      OrderExpr(FuncExpr("xxHash64", List(FieldRef("k"))), asc = true, nullFirst = false)
    ))
  }

  test("renderOrder: bare field name that is a CH reserved word is back-quoted") {
    val rendered = ExprRender.renderOrder(
      OrderExpr(FieldRef("order"), asc = true, nullFirst = false)
    )
    assert(rendered === "`order` ASC NULLS LAST")
  }

  test("renderOrder: function-transform leaf field is back-quoted") {
    val rendered = ExprRender.renderOrder(
      OrderExpr(FuncExpr("xxHash64", List(FieldRef("order"))), asc = true, nullFirst = false)
    )
    assert(rendered === "xxHash64(`order`) ASC NULLS LAST")
  }

  test("renderOrder: SQLExpr leaf is emitted verbatim (e.g. literal arg)") {
    val rendered = ExprRender.renderOrder(
      OrderExpr(FuncExpr("toStartOfInterval", List(FieldRef("ts"), SQLExpr("INTERVAL 1 HOUR"))), asc = false)
    )
    assert(rendered === "toStartOfInterval(`ts`,INTERVAL 1 HOUR) DESC NULLS LAST")
  }

  test("toClickHouseSortOrderOpt + renderOrder: end-to-end with reserved-word column under xxHash64") {
    val sort = Expressions.sort(
      Expressions.apply("ck_xx_hash64", Expressions.column("order")),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    val translated = ExprUtils.toClickHouseSortOrderOpt(sort, registry)
    assert(translated.isDefined)
    assert(ExprRender.renderOrder(translated.get) === "xxHash64(`order`) ASC NULLS LAST")
  }

  test("toClickHouseSortOrderOpt: function transform NOT in registry returns None") {
    val sort = Expressions.sort(
      Expressions.apply("totally_unknown_func", Expressions.column("k")),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, StaticFunctionRegistry).isEmpty)
  }

  test("toClickHouseSortOrderOpt: literal expression returns None") {
    val sort = Expressions.sort(
      LiteralValue[Integer](Integer.valueOf(1), IntegerType),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, StaticFunctionRegistry).isEmpty)
  }

  test("toClickHouseSortOrderOpt: arbitrary GeneralScalarExpression returns None") {
    val sort = Expressions.sort(
      new GeneralScalarExpression("=", Array(Expressions.column("k"), LiteralValue(1, IntegerType))),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, StaticFunctionRegistry).isEmpty)
  }

  test("toClickHouseSortOrderOpt: nested function transform recurses and back-quotes leaf") {
    val sort = Expressions.sort(
      Expressions.apply("ck_xx_hash64", Expressions.apply("ck_xx_hash64", Expressions.column("order"))),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    val translated = ExprUtils.toClickHouseSortOrderOpt(sort, registry)
    assert(translated === Some(
      OrderExpr(
        FuncExpr("xxHash64", List(FuncExpr("xxHash64", List(FieldRef("order"))))),
        asc = true,
        nullFirst = false
      )
    ))
    assert(ExprRender.renderOrder(translated.get) ===
      "xxHash64(xxHash64(`order`)) ASC NULLS LAST")
  }

  test("toClickHouseSortOrderOpt: literal arg inside function transform is preserved") {
    val sort = Expressions.sort(
      Expressions.apply("ck_xx_hash64", LiteralValue(42, IntegerType)),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    val translated = ExprUtils.toClickHouseSortOrderOpt(sort, registry)
    assert(translated === Some(
      OrderExpr(FuncExpr("xxHash64", List(SQLExpr("42"))), asc = true, nullFirst = false)
    ))
  }

  test("toClickHouseSortOrderOpt: nested unregistered function transform returns None") {
    val sort = Expressions.sort(
      Expressions.apply("ck_xx_hash64", Expressions.apply("totally_unknown_func", Expressions.column("k"))),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, registry).isEmpty)
  }

  test("toClickHouseSortOrderOpt: arbitrary V2 expression as function arg returns None") {
    val sort = Expressions.sort(
      Expressions.apply(
        "ck_xx_hash64",
        new GeneralScalarExpression("+", Array(Expressions.column("k"), LiteralValue(1, IntegerType)))
      ),
      SortDirection.ASCENDING,
      NullOrdering.NULLS_LAST
    )
    assert(ExprUtils.toClickHouseSortOrderOpt(sort, registry).isEmpty)
  }
}
