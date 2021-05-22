package xenon.clickhouse

import org.scalatest.funsuite.AnyFunSuite

class TableEngineUtilSuite extends AnyFunSuite {

  test("parse MergeTree - 1") {
    val ddl = "MergeTree()"
    val tableEngine = TableEngineUtil.parseMergeTreeEngine(ddl)
    assert(tableEngine.engine_expr == "MergeTree()")
  }

  test("parse ReplicatedMergeTree - 1") {
    val ddl = "ReplicatedMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}')"
    val tableEngine = TableEngineUtil.parseReplicatedMergeTreeEngine(ddl)
    assert(tableEngine.zk_path == "/clickhouse/tables/{shard}/wj_report/wj_respondent")
    assert(tableEngine.replica_name == "{replica}")
  }

  test("parse ReplacingMergeTree - 1") {
    val ddl = "ReplacingMergeTree()"
    val tableEngine = TableEngineUtil.parseReplacingMergeTreeEngine(ddl)
    assert(tableEngine.version_column.isEmpty)
  }

  test("parse ReplacingMergeTree - 2") {
    val ddl = "ReplacingMergeTree(ts)"
    val tableEngine = TableEngineUtil.parseReplacingMergeTreeEngine(ddl)
    assert(tableEngine.version_column.contains("ts"))
  }

  test("parse ReplicatedReplacingMergeTree - 1") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}')"
    val tableEngine = TableEngineUtil.parseReplicatedReplacingMergeTreeEngine(ddl)
    assert(tableEngine.zk_path == "/clickhouse/tables/{shard}/wj_report/wj_respondent")
    assert(tableEngine.replica_name == "{replica}")
    assert(tableEngine.version_column.isEmpty)
  }

  test("parse ReplicatedReplacingMergeTree - 2") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts)"
    val tableEngine = TableEngineUtil.parseReplicatedReplacingMergeTreeEngine(ddl)
    assert(tableEngine.zk_path == "/clickhouse/tables/{shard}/wj_report/wj_respondent")
    assert(tableEngine.replica_name == "{replica}")
    assert(tableEngine.version_column.contains("ts"))
  }

  test("parse Distributed - 1") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local')"
    val tableEngine = TableEngineUtil.parseDistributedEngine(ddl)
    assert(tableEngine.cluster == "default")
    assert(tableEngine.local_db == "wj_report")
    assert(tableEngine.local_table == "wj_respondent_local")
    assert(tableEngine.sharding_key.isEmpty)
    assert(tableEngine.storage_policy.isEmpty)
  }

  test("parse Distributed - 2") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))"
    val tableEngine = TableEngineUtil.parseDistributedEngine(ddl)
    assert(tableEngine.cluster == "default")
    assert(tableEngine.local_db == "wj_report")
    assert(tableEngine.local_table == "wj_respondent_local")
    assert(tableEngine.sharding_key.contains("xxHash64(id)"))
    assert(tableEngine.storage_policy.isEmpty)
  }
}
