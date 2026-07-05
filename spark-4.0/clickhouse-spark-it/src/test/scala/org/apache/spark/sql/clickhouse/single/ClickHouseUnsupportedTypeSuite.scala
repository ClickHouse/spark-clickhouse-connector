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

package org.apache.spark.sql.clickhouse.single

import com.clickhouse.spark.base.ClickHouseSingleMixIn
import org.apache.spark.sql.Row

class ClickHouseSingleUnsupportedTypeSuite extends ClickHouseUnsupportedTypeSuite with ClickHouseSingleMixIn

/**
 * Regression suite for https://github.com/ClickHouse/spark-clickhouse-connector/issues/457.
 *
 * A table may contain columns whose ClickHouse types have no Spark mapping (e.g.
 * `AggregateFunction`). Queries that do not reference such columns must still succeed:
 * schema validation should only apply to the columns a query actually reads, not to the
 * whole table schema during planning.
 */
abstract class ClickHouseUnsupportedTypeSuite extends SparkClickHouseSingleTest {

  test("query only supported columns of a table containing unsupported column types") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "issue_457_db"
    val tbl = "issue_457_tbl"
    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      }
      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |  customer_id Int32,
           |  client_id   String,
           |  agg_state   AggregateFunction(sum, Int32),
           |  location    Point
           |) ENGINE = MergeTree()
           |ORDER BY customer_id
           |""".stripMargin
      )
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` (customer_id, client_id) VALUES (1, 'a'), (2, 'b')")

      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` ORDER BY customer_id"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
      }
  }
}
