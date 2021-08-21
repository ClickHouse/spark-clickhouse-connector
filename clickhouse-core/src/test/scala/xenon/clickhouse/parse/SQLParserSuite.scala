package xenon.clickhouse.parse

import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.expr.{FieldRef, FuncExpr, OrderExpr, TupleExpr}
import xenon.clickhouse.spec.MergeTreeEngineSpecV2

class SQLParserSuite extends AnyFunSuite {

  val parser = new SQLParser(new AstVisitor)

  test("parse MergeTree - 1") {
    val ddl = "MergeTree PARTITION BY toYYYYMM(create_time) ORDER BY id"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpecV2(
      engine_expr = "MergeTree",
      _sorting_key = List(OrderExpr(FieldRef("id"))),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("create_time")))))
    )
    assert(actual === expected)
  }

  test("parse ReplicatedMergeTree - 1") {
    val ddl = "ReplicatedMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    parser.parseEngineClause(ddl)
  }

  test("parse ReplacingMergeTree - 1") {
    val ddl = "ReplacingMergeTree() " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    parser.parseEngineClause(ddl)
  }

  test("parse ReplacingMergeTree - 2") {
    val ddl = "ReplacingMergeTree(ts) " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    parser.parseEngineClause(ddl)
  }

  test("parse ReplicatedReplacingMergeTree - 1") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    parser.parseEngineClause(ddl)
  }

  test("parse ReplicatedReplacingMergeTree - 2") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts) " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    parser.parseEngineClause(ddl)
  }

  test("parse Distributed - 1") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local')"
    parser.parseEngineClause(ddl)
  }

  test("parse Distributed - 2") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))"
    parser.parseEngineClause(ddl)
  }

  test("parse Distributed - 3") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(toString(id, ver)))"
    parser.parseEngineClause(ddl)
  }
}
