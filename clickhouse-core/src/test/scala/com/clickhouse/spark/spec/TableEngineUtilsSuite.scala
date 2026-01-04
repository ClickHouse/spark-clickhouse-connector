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
