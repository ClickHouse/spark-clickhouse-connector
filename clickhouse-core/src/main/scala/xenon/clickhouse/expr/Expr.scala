package xenon.clickhouse.expr

trait Expr {
  def desc: String

  override def toString: String = desc
}

case class StringLiteral(value: String) extends Expr {
  override def desc: String = value
}

case class FieldExpr(name: String) extends Expr {
  override def desc: String = name
}

case class FuncExpr(name: String, args: Array[Expr]) extends Expr {
  override def desc: String = s"$name(${args.map(_.desc).mkString(",")})"
}
