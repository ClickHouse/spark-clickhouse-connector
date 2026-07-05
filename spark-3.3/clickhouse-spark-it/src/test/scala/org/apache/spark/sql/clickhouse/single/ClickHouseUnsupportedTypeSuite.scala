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
import org.apache.spark.sql.{AnalysisException, Row}

class ClickHouseSingleUnsupportedTypeSuite extends ClickHouseUnsupportedTypeSuite with ClickHouseSingleMixIn

/**
 * Regression suite for https://github.com/ClickHouse/spark-clickhouse-connector/issues/457.
 *
 * A table may contain columns whose ClickHouse types have no Spark mapping (e.g.
 * `AggregateFunction`). Such columns are excluded from the Spark table schema, so the
 * remaining columns stay readable and writable; explicitly referencing an excluded column
 * fails with Spark's unresolved-column error. The excluded columns are listed in the
 * `unsupported.columns` table property.
 */
abstract class ClickHouseUnsupportedTypeSuite extends SparkClickHouseSingleTest {

  import testImplicits._

  private def withUnsupportedTypeTable(f: (String, String) => Unit): Unit = {
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

      f(db, tbl)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  test("query only supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` ORDER BY customer_id"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )
    }
  }

  test("SELECT * excludes columns with unsupported types") {
    withUnsupportedTypeTable { (db, tbl) =>
      val df = spark.sql(s"SELECT * FROM `$db`.`$tbl` ORDER BY customer_id")
      assert(df.schema.fieldNames === Array("customer_id", "client_id"))
      checkAnswer(df, Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("unsupported columns are exposed as a table property") {
    withUnsupportedTypeTable { (db, tbl) =>
      val props = spark.sql(s"SHOW TBLPROPERTIES `$db`.`$tbl`")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap
      assert(props("unsupported.columns") === "`agg_state` AggregateFunction(sum, Int32), `location` Point")
    }
  }

  test("referencing an unsupported column fails with unresolved column error") {
    withUnsupportedTypeTable { (db, tbl) =>
      val e = intercept[AnalysisException] {
        spark.sql(s"SELECT agg_state FROM `$db`.`$tbl`").collect()
      }
      assert(e.getMessage.contains("agg_state"))
    }
  }

  test("write to supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      Seq((3, "c"), (4, "d")).toDF("customer_id", "client_id")
        .writeTo(s"$db.$tbl")
        .append()
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` ORDER BY customer_id"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil
      )
    }
  }
}
