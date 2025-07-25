package com.clickhouse.spark.spec

import org.scalatest.funsuite.AnyFunSuite

class TableEngineUtilsSuite extends AnyFunSuite with NodeSpecHelper {

  test("test resolve table cluster by macro") {
    val distributeSpec = DistributedEngineSpec(
      engine_clause = "Distributed('{cluster}', 'wj_report', 'wj_respondent_local')",
      cluster = "{cluster}",
      local_db = "wj_report",
      local_table = "wj_respondent_local",
      sharding_key = None
    )
    val clusterName = TableEngineUtils
      .resolveTableCluster(distributeSpec, Seq(cluster), Seq(MacrosSpec("cluster", cluster_name)))

    assert(clusterName.name === cluster_name)
  }
}
