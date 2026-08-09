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

import com.clickhouse.spark.LogCaptureHelper
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class TableEngineUtilsSuite extends AnyFunSuite with NodeSpecHelper with LogCaptureHelper {

  private val engineUtilsLogger = TableEngineUtils.getClass.getName.stripSuffix("$")

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

  test("resolving a view does not warn about unknown table engine") {
    val (engineSpec, warnings) =
      captureWarnings(engineUtilsLogger)(TableEngineUtils.resolveTableEngine(tableSpec(engine = "View")))
    assert(engineSpec === UnknownTableEngineSpec(""))
    assert(warnings.isEmpty)
  }

  test("resolving an unparseable engine of a non-view table warns about unknown table engine") {
    val (engineSpec, warnings) = captureWarnings(engineUtilsLogger) {
      TableEngineUtils.resolveTableEngine(tableSpec(engine = "Memory", engineFull = ""))
    }
    assert(engineSpec === UnknownTableEngineSpec(""))
    assert(warnings.exists(_.contains("Unknown table engine for table db.tbl")))
  }

  // a View reports an empty engine_full, which is unparseable
  private def tableSpec(engine: String, engineFull: String = ""): TableSpec = TableSpec(
    database = "db",
    name = "tbl",
    uuid = "",
    engine = engine,
    is_temporary = false,
    data_paths = Nil,
    metadata_path = "",
    metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
    dependencies_database = Nil,
    dependencies_table = Nil,
    create_table_query = "",
    engine_full = engineFull,
    partition_key = "",
    sorting_key = "",
    primary_key = "",
    sampling_key = "",
    storage_policy = "",
    total_rows = None,
    total_bytes = None,
    lifetime_rows = None,
    lifetime_bytes = None
  )
}
