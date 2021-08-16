package xenon.clickhouse.expr

trait Expr {
  def sql: String
  def desc: String = s"${this.getClass.getSimpleName.stripSuffix("$")}[$sql]"
  override def toString: String = desc
}

case class StringLiteral(value: String) extends Expr {
  override def sql: String = value
}

case class FieldRef(name: String) extends Expr {
  override def sql: String = name
}

case class FuncExpr(name: String, args: Array[Expr]) extends Expr {
  override def sql: String = s"$name(${args.map(_.desc).mkString(",")})"
}

case class OrderExpr(expr: Expr, asc: Boolean, nullFirst: Boolean) extends Expr {
  override def sql: String = s"$expr ${if (asc) "ASC" else "DESC"} NULLS ${if (nullFirst) "FIRST" else "LAST"}"
}

case class TupleExpr(exprList: Array[Expr]) extends Expr {
  override def sql: String = exprList.mkString("(", ",", ")")
}
