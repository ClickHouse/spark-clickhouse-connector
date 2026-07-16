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
import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.clickhouse.ClickHouseUnsupportedType

class ClickHouseSingleUnsupportedTypeSuite extends ClickHouseUnsupportedTypeSuite with ClickHouseSingleMixIn

/**
 * Regression suite for https://github.com/ClickHouse/spark-clickhouse-connector/issues/457.
 *
 * A table may contain columns whose ClickHouse types have no Spark mapping (e.g.
 * `AggregateFunction`). Such columns appear in the Spark table schema with the placeholder
 * `unsupported` type, so queries and writes touching only supported columns succeed, while
 * reading an unsupported column - explicitly or via `SELECT *` - or writing to it fails
 * with an error naming the columns. The columns are also listed in the
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
           |  avg_state   AggregateFunction(avg, Float64)
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

  test("unsupported columns stay in the schema with the placeholder type") {
    withUnsupportedTypeTable { (db, tbl) =>
      val schema = spark.table(s"`$db`.`$tbl`").schema
      assert(schema.fieldNames === Array("customer_id", "client_id", "agg_state", "avg_state"))
      assert(ClickHouseUnsupportedType.unsupportedColumns(schema) === Seq(
        "agg_state" -> "AggregateFunction(sum, Int32)",
        "avg_state" -> "AggregateFunction(avg, Float64)"
      ))
    }
  }

  test("query only supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` ORDER BY customer_id"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )
      checkAnswer(
        spark.sql(s"SELECT count(*) FROM `$db`.`$tbl`"),
        Row(2) :: Nil
      )
    }
  }

  test("SELECT * works when all columns are supported") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "issue_457_ctl_db"
    val tbl = "issue_457_ctl_tbl"
    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      }
      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |  customer_id Int32,
           |  client_id   String
           |) ENGINE = MergeTree()
           |ORDER BY customer_id
           |""".stripMargin
      )
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 'a'), (2, 'b')")

      val df = spark.sql(s"SELECT * FROM `$db`.`$tbl` ORDER BY customer_id")
      assert(df.schema.fieldNames === Array("customer_id", "client_id"))
      checkAnswer(df, Row(1, "a") :: Row(2, "b") :: Nil)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  test("SELECT * fails when the table contains unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      val e = intercept[CHClientException] {
        spark.sql(s"SELECT * FROM `$db`.`$tbl`").collect()
      }
      assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
      assert(e.getMessage.contains("`avg_state` AggregateFunction(avg, Float64)"))
    }
  }

  test("selecting an unsupported column explicitly fails") {
    withUnsupportedTypeTable { (db, tbl) =>
      val e = intercept[CHClientException] {
        spark.sql(s"SELECT agg_state FROM `$db`.`$tbl`").collect()
      }
      assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
      assert(!e.getMessage.contains("`avg_state`"))
    }
  }

  test("aggregating an unsupported column fails") {
    withUnsupportedTypeTable { (db, tbl) =>
      // sum requires a numeric input, so Spark's analyzer rejects it before the connector is consulted
      val e1 = intercept[AnalysisException] {
        spark.sql(s"SELECT sum(agg_state) FROM `$db`.`$tbl`").collect()
      }
      assert(e1.getMessage.contains("agg_state"))

      // max accepts any orderable type, so it reaches the connector: the pushdown probe is
      // declined and the fallback in-Spark aggregate hits the read guard on the column
      val e2 = intercept[CHClientException] {
        spark.sql(s"SELECT max(avg_state) FROM `$db`.`$tbl`").collect()
      }
      assert(e2.getMessage.contains("`avg_state` AggregateFunction(avg, Float64)"))
    }
  }

  test("aggregating supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      // column-referencing aggregates must still be pushed down / answered despite the placeholders
      checkAnswer(
        spark.sql(s"SELECT max(customer_id), min(customer_id) FROM `$db`.`$tbl`"),
        Row(2, 1) :: Nil
      )
      checkAnswer(
        spark.sql(s"SELECT client_id, sum(customer_id) FROM `$db`.`$tbl` GROUP BY client_id ORDER BY client_id"),
        Row("a", 1L) :: Row("b", 2L) :: Nil
      )
    }
  }

  test("filter pushdown on supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` WHERE customer_id = 1"),
        Row(1, "a") :: Nil
      )
    }
  }

  test("null-test filters on unsupported columns are pushed down and evaluated by ClickHouse") {
    withUnsupportedTypeTable { (db, tbl) =>
      // the columns are not Nullable in ClickHouse, so IS NULL matches nothing and IS NOT NULL
      // everything; the filters are fully pushed, so the columns are never read into Spark
      checkAnswer(
        spark.sql(s"SELECT customer_id FROM `$db`.`$tbl` WHERE agg_state IS NULL"),
        Nil
      )
      checkAnswer(
        spark.sql(s"SELECT customer_id FROM `$db`.`$tbl` WHERE avg_state IS NOT NULL ORDER BY customer_id"),
        Row(1) :: Row(2) :: Nil
      )
    }
  }

  test("limit pushdown on a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      assert(spark.sql(s"SELECT customer_id FROM `$db`.`$tbl` LIMIT 1").collect().length === 1)
    }
  }

  test("top-N pushdown on supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` ORDER BY customer_id DESC LIMIT 1"),
        Row(2, "b") :: Nil
      )
    }
  }

  test("ordering by an unsupported column fails") {
    withUnsupportedTypeTable { (db, tbl) =>
      // top-N form: the order may be pushed, but Spark still reads the column for the final merge,
      // so the read guard fires before any SQL reaches ClickHouse
      val e1 = intercept[CHClientException] {
        spark.sql(s"SELECT customer_id FROM `$db`.`$tbl` ORDER BY agg_state LIMIT 1").collect()
      }
      assert(e1.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
      // plain sort (no pushdown): Spark itself needs the column
      val e2 = intercept[CHClientException] {
        spark.sql(s"SELECT customer_id FROM `$db`.`$tbl` ORDER BY avg_state").collect()
      }
      assert(e2.getMessage.contains("`avg_state` AggregateFunction(avg, Float64)"))
    }
  }

  test("runtime filter on supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      spark.sql("set spark.clickhouse.read.runtimeFilter.enabled=true")
      try
        checkAnswer(
          spark.sql(
            s"SELECT customer_id FROM `$db`.`$tbl` WHERE customer_id IN " +
              s"(SELECT customer_id FROM `$db`.`$tbl` WHERE client_id = 'a')"
          ),
          Row(1) :: Nil
        )
      finally
        spark.sql("set spark.clickhouse.read.runtimeFilter.enabled=false")
    }
  }

  test("dropping the unsupported columns from the DataFrame works") {
    withUnsupportedTypeTable { (db, tbl) =>
      // the remedy the read-guard error message suggests
      checkAnswer(
        spark.table(s"`$db`.`$tbl`").drop("agg_state", "avg_state").orderBy("customer_id"),
        Row(1, "a") :: Row(2, "b") :: Nil
      )
    }
  }

  test("DESCRIBE TABLE shows the placeholder type for unsupported columns") {
    withUnsupportedTypeTable { (db, tbl) =>
      val columnTypes = spark.sql(s"DESCRIBE TABLE `$db`.`$tbl`")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap
      assert(columnTypes("customer_id") === "int")
      assert(columnTypes("agg_state") === "unsupported")
      assert(columnTypes("avg_state") === "unsupported")
    }
  }

  test("unsupported columns are exposed as a table property") {
    withUnsupportedTypeTable { (db, tbl) =>
      val props = spark.sql(s"SHOW TBLPROPERTIES `$db`.`$tbl`")
        .collect()
        .map(row => row.getString(0) -> row.getString(1))
        .toMap
      assert(props(
        "unsupported.columns"
      ) === "`agg_state` AggregateFunction(sum, Int32), `avg_state` AggregateFunction(avg, Float64)")
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

  test("SQL INSERT INTO supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      // Spark 3.3 does not rewrite a column-list INSERT for ACCEPT_ANY_SCHEMA v2 tables,
      // so the query output must carry matching column names itself
      spark.sql(s"INSERT INTO `$db`.`$tbl` SELECT 3 AS customer_id, 'c' AS client_id")
      spark.sql(
        s"INSERT INTO `$db`.`$tbl` SELECT customer_id + 3 AS customer_id, 'd' AS client_id FROM `$db`.`$tbl` WHERE customer_id = 1"
      )
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl` ORDER BY customer_id"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil
      )
    }
  }

  test("SQL INSERT INTO without a column list fails and writes nothing") {
    withUnsupportedTypeTable { (db, tbl) =>
      // by-ordinal insert must provide all 4 columns, including the unsupported ones
      intercept[Exception] {
        spark.sql(s"INSERT INTO `$db`.`$tbl` VALUES (5, 'e', 'x', 'y')")
      }
      checkAnswer(spark.sql(s"SELECT count(*) FROM `$db`.`$tbl`"), Row(2) :: Nil)
    }
  }

  test("overwrite with supported columns of a table containing unsupported column types") {
    withUnsupportedTypeTable { (db, tbl) =>
      Seq((9, "z")).toDF("customer_id", "client_id")
        .writeTo(s"$db.$tbl")
        .overwrite(org.apache.spark.sql.functions.lit(true))
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM `$db`.`$tbl`"),
        Row(9, "z") :: Nil
      )
    }
  }

  test("writing to an unsupported column fails") {
    withUnsupportedTypeTable { (db, tbl) =>
      val e = intercept[CHClientException] {
        Seq((5, "e", "x")).toDF("customer_id", "client_id", "agg_state")
          .writeTo(s"$db.$tbl")
          .append()
      }
      assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
    }
  }
}
