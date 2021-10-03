/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.expr

trait Expr extends Serializable {
  def sql: String // ClickHouse dialect
  def sparkSql: String = sql
  def desc: String = s"${this.getClass.getSimpleName.stripSuffix("$")}[$sql]"
  override def toString: String = desc
}

case class StringLiteral(value: String) extends Expr {
  override def sql: String = s"'$value'"
}

case class FieldRef(name: String) extends Expr {
  override def sql: String = name
}

case class FuncExpr(name: String, args: List[Expr]) extends Expr {
  override def sql: String = s"$name(${args.map(_.desc).mkString(",")})"
}

case class OrderExpr(expr: Expr, asc: Boolean = true, nullFirst: Boolean = true) extends Expr {
  override def sql: String = s"$expr ${if (asc) "ASC" else "DESC"} NULLS ${if (nullFirst) "FIRST" else "LAST"}"
}

case class TupleExpr(exprList: List[Expr]) extends Expr {
  override def sql: String = exprList.mkString("(", ",", ")")
}
