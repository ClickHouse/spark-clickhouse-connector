package xenon.clickhouse.write

object WriteAction extends Enumeration {
  type WriteAction = Value
  val APPEND, TRUNCATE = Value
}
