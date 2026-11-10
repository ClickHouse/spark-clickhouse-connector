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

import java.time.LocalDateTime

/** Table specs for tests: the fields a test reads are set, the rest are blank. */
object TableSpecs {

  def tableSpec(
    database: String = "db",
    name: String = "tbl",
    engine: String = "MergeTree",
    partitionKey: String = "",
    primaryKey: String = ""
  ): TableSpec = TableSpec(
    database = database,
    name = name,
    uuid = "",
    engine = engine,
    is_temporary = false,
    data_paths = Nil,
    metadata_path = "",
    metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
    dependencies_database = Nil,
    dependencies_table = Nil,
    create_table_query = "",
    engine_full = engine,
    partition_key = partitionKey,
    sorting_key = primaryKey,
    primary_key = primaryKey,
    sampling_key = "",
    storage_policy = "",
    total_rows = None,
    total_bytes = None,
    lifetime_rows = None,
    lifetime_bytes = None
  )
}
