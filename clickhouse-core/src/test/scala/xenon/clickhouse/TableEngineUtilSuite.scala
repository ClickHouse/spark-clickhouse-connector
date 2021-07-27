package xenon.clickhouse

import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.parse.LegacyTableEngineParser

class TableEngineUtilSuite extends AnyFunSuite {

  test("parse MergeTree - 1") {
    val ddl = "MergeTree PARTITION BY toYYYYMM(create_time) ORDER BY id"
    val tableEngine = LegacyTableEngineParser.parseMergeTreeEngine(ddl)
    assert(tableEngine.engine_expr.startsWith("MergeTree"))
  }

  test("parse ReplicatedMergeTree - 1") {
    val ddl = "ReplicatedMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val tableEngine = LegacyTableEngineParser.parseReplicatedMergeTreeEngine(ddl)
    assert(tableEngine.zk_path == "/clickhouse/tables/{shard}/wj_report/wj_respondent")
    assert(tableEngine.replica_name == "{replica}")
  }

  test("parse ReplacingMergeTree - 1") {
    val ddl = "ReplacingMergeTree() " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val tableEngine = LegacyTableEngineParser.parseReplacingMergeTreeEngine(ddl)
    assert(tableEngine.version_column.isEmpty)
  }

  test("parse ReplacingMergeTree - 2") {
    val ddl = "ReplacingMergeTree(ts) " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val tableEngine = LegacyTableEngineParser.parseReplacingMergeTreeEngine(ddl)
    assert(tableEngine.version_column.contains("ts"))
  }

  test("parse ReplicatedReplacingMergeTree - 1") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}') " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val tableEngine = LegacyTableEngineParser.parseReplicatedReplacingMergeTreeEngine(ddl)
    assert(tableEngine.zk_path == "/clickhouse/tables/{shard}/wj_report/wj_respondent")
    assert(tableEngine.replica_name == "{replica}")
    assert(tableEngine.version_column.isEmpty)
  }

  test("parse ReplicatedReplacingMergeTree - 2") {
    val ddl = "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts) " +
      "PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192"
    val tableEngine = LegacyTableEngineParser.parseReplicatedReplacingMergeTreeEngine(ddl)
    assert(tableEngine.zk_path == "/clickhouse/tables/{shard}/wj_report/wj_respondent")
    assert(tableEngine.replica_name == "{replica}")
    assert(tableEngine.version_column.contains("ts"))
  }

  test("parse Distributed - 1") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local')"
    val tableEngine = LegacyTableEngineParser.parseDistributedEngine(ddl)
    assert(tableEngine.cluster == "default")
    assert(tableEngine.local_db == "wj_report")
    assert(tableEngine.local_table == "wj_respondent_local")
    assert(tableEngine.sharding_key.isEmpty)
    assert(tableEngine.storage_policy.isEmpty)
  }

  test("parse Distributed - 2") {
    val ddl = "Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))"
    val tableEngine = LegacyTableEngineParser.parseDistributedEngine(ddl)
    assert(tableEngine.cluster == "default")
    assert(tableEngine.local_db == "wj_report")
    assert(tableEngine.local_table == "wj_respondent_local")
    assert(tableEngine.sharding_key.contains("xxHash64(id)"))
    assert(tableEngine.storage_policy.isEmpty)
  }
}
