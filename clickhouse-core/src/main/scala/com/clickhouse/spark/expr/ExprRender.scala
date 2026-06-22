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

package com.clickhouse.spark.expr

import com.clickhouse.spark.Utils

/**
 * Renders `Expr` trees as ClickHouse SQL with identifier back-quoting.
 *
 * Use this on the pushdown path (e.g. `ORDER BY` in scan queries) where
 * connector-emitted identifiers must round-trip safely even when they
 * collide with ClickHouse reserved words. The DDL path keeps using
 * `Expr.sql` directly because the user owns identifier quoting there.
 */
object ExprRender {

  def render(expr: Expr): String = expr match {
    case FieldRef(name) => Utils.wrapBackQuote(name)
    case FuncExpr(name, args) => s"$name(${args.map(render).mkString(",")})"
    case other => other.sql
  }

  def renderOrder(o: OrderExpr): String = {
    val direction = if (o.asc) "ASC" else "DESC"
    val nulls = if (o.nullFirst) "NULLS FIRST" else "NULLS LAST"
    s"${render(o.expr)} $direction $nulls"
  }
}
