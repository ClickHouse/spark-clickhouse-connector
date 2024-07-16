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

package xenon.clickhouse.parse

import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.expr._
import xenon.clickhouse.spec._

class SQLParserSuite extends AnyFunSuite {

  val parser = new SQLParser(new AstVisitor)

  test("parse MergeTree - 1") {
    val ddl = "MergeTree PARTITION BY toYYYYMM(create_time) ORDER BY id"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpec(
      engine_clause = "MergeTree",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("create_time")))))
    )
    assert(actual === expected)
  }

  test("parse ReplicatedMergeTree - 1") {
    val ddl = "ReplicatedMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val actual = parser.parseEngineClause(ddl)
    val expected = ReplicatedMergeTreeEngineSpec(
      engine_clause = "ReplicatedMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}')",
      zk_path = "/clickhouse/tables/{shard}/wj_report/wj_respondent",
      replica_name = "{replica}",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("created"))))),
      _settings = Map("index_granularity" -> "8192")
    )
    assert(actual === expected)
  }

  test("parse ReplacingMergeTree - 1") {
    val ddl = "ReplacingMergeTree() " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val actual = parser.parseEngineClause(ddl)
    val expected = ReplacingMergeTreeEngineSpec(
      engine_clause = "ReplacingMergeTree()",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("created"))))),
      _settings = Map("index_granularity" -> "8192")
    )
    assert(actual === expected)
  }

  test("parse ReplacingMergeTree - 2") {
    val ddl = "ReplacingMergeTree(ts) " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val actual = parser.parseEngineClause(ddl)
    val expected = ReplacingMergeTreeEngineSpec(
      engine_clause = "ReplacingMergeTree(ts)",
      version_column = Some(FieldRef("ts")),
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("created"))))),
      _settings = Map("index_granularity" -> "8192")
    )
    assert(actual === expected)
  }

  test("parse ReplicatedReplacingMergeTree - 1") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val actual = parser.parseEngineClause(ddl)
    val expected = ReplicatedReplacingMergeTreeEngineSpec(
      engine_clause = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}')",
      zk_path = "/clickhouse/tables/{shard}/wj_report/wj_respondent",
      replica_name = "{replica}",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("created"))))),
      _settings = Map("index_granularity" -> "8192")
    )
    assert(actual === expected)
  }

  test("parse ReplicatedReplacingMergeTree - 2") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts) " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val actual = parser.parseEngineClause(ddl)
    val expected = ReplicatedReplacingMergeTreeEngineSpec(
      engine_clause =
        "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts)",
      zk_path = "/clickhouse/tables/{shard}/wj_report/wj_respondent",
      replica_name = "{replica}",
      version_column = Some(FieldRef("ts")),
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("created"))))),
      _settings = Map("index_granularity" -> "8192")
    )
    assert(actual === expected)
  }

  test("parse Distributed - 1") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local')"
    val actual = parser.parseEngineClause(ddl)
    val expected = DistributedEngineSpec(
      engine_clause = "Distributed('default', 'wj_report', 'wj_respondent_local')",
      cluster = "default",
      local_db = "wj_report",
      local_table = "wj_respondent_local",
      sharding_key = None
    )
    assert(actual === expected)
  }

  test("parse Distributed - 2") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))"
    val actual = parser.parseEngineClause(ddl)
    val expected = DistributedEngineSpec(
      engine_clause = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))",
      cluster = "default",
      local_db = "wj_report",
      local_table = "wj_respondent_local",
      sharding_key = Some(FuncExpr("xxHash64", List(FieldRef("id"))))
    )
    assert(actual === expected)
  }

  test("parse Distributed - 3") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(toString(id, ver)))"
    val actual = parser.parseEngineClause(ddl)
    val expected = DistributedEngineSpec(
      engine_clause = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(toString(id, ver)))",
      cluster = "default",
      local_db = "wj_report",
      local_table = "wj_respondent_local",
      sharding_key = Some(FuncExpr("xxHash64", List(FuncExpr("toString", List(FieldRef("id"), FieldRef("ver"))))))
    )
    assert(actual === expected)
  }

  test("parse MergeTree - partition by tuple - 1") {
    val ddl = "MergeTree PARTITION BY (a) ORDER BY id"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpec(
      engine_clause = "MergeTree",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(FieldRef("a") :: Nil)
    )
    assert(actual === expected)
  }

  test("parse MergeTree - partition by tuple - 2") {
    val ddl = "MergeTree PARTITION BY (a, b) ORDER BY id"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpec(
      engine_clause = "MergeTree",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(FieldRef("a") :: FieldRef("b") :: Nil)
    )
    assert(actual === expected)
  }

  test("parse MergeTree - order by tuple - 1") {
    val ddl = "MergeTree ORDER BY (a)"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpec(
      engine_clause = "MergeTree",
      _sorting_key = TupleExpr(FieldRef("a") :: Nil)
    )
    assert(actual === expected)
  }

  test("parse MergeTree - order by tuple - 2") {
    val ddl = "MergeTree ORDER BY (a, b)"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpec(
      engine_clause = "MergeTree",
      _sorting_key = TupleExpr(FieldRef("a") :: FieldRef("b") :: Nil)
    )
    assert(actual === expected)
  }

  test("parse MergeTree - partition by %") {
    val ddl = "MergeTree PARTITION BY id % 3"
    val actual = parser.parseEngineClause(ddl)
    val expected = MergeTreeEngineSpec(
      engine_clause = "MergeTree",
      _partition_key = TupleExpr(FuncExpr("remainder", FieldRef("id") :: StringLiteral("3") :: Nil) :: Nil)
    )
    assert(actual === expected)
  }

  test("parse SharedMergeTree - 1") {
    val ddl = "SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val actual = parser.parseEngineClause(ddl)
    val expected = SharedMergeTreeEngineSpec(
      engine_clause = "SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')",
      zk_path = "/clickhouse/tables/{uuid}/{shard}",
      replica_name = "{replica}",
      _sorting_key = TupleExpr(FieldRef("id") :: Nil),
      _partition_key = TupleExpr(List(FuncExpr("toYYYYMM", List(FieldRef("created"))))),
      _settings = Map("index_granularity" -> "8192")
    )
    assert(actual === expected)
  }

}
