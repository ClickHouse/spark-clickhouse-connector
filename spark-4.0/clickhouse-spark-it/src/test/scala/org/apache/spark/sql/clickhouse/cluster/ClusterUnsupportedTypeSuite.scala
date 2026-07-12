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

package org.apache.spark.sql.clickhouse.cluster

import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.sql.Row
import org.apache.spark.sql.clickhouse.ClickHouseUnsupportedType

/**
 * Distributed-engine counterpart of the single-node `ClickHouseUnsupportedTypeSuite`:
 * unsupported columns of a Distributed table (and its local tables) stay in the Spark
 * schema with the placeholder type; only queries touching them fail.
 */
class ClusterUnsupportedTypeSuite extends SparkClickHouseClusterTest {

  private def withUnsupportedTypeDistTable(db: String)(f: (String, String) => Unit): Unit =
    autoCleanupDistTable("single_replica", db, "issue_457_dist") { (cluster, db, tbl_dist, tbl_local) =>
      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl_local ON CLUSTER $cluster (
           |  customer_id Int32,
           |  client_id   String,
           |  agg_state   AggregateFunction(sum, Int32),
           |  location    Point
           |) ENGINE = MergeTree()
           |ORDER BY customer_id
           |""".stripMargin
      )
      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl_dist ON CLUSTER $cluster (
           |  customer_id Int32,
           |  client_id   String,
           |  agg_state   AggregateFunction(sum, Int32),
           |  location    Point
           |) ENGINE = Distributed('$cluster', '$db', '$tbl_local', customer_id)
           |""".stripMargin
      )
      runClickHouseSQL(
        s"INSERT INTO $db.$tbl_dist (customer_id, client_id) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
      )
      runClickHouseSQL(s"SYSTEM FLUSH DISTRIBUTED $db.$tbl_dist")
      f(db, tbl_dist)
    }

  test("unsupported columns of a distributed table stay in the schema with the placeholder type") {
    withUnsupportedTypeDistTable("issue_457_cluster_schema_db") { (db, tbl) =>
      val schema = spark.table(s"$db.$tbl").schema
      assert(schema.fieldNames === Array("customer_id", "client_id", "agg_state", "location"))
      assert(ClickHouseUnsupportedType.unsupportedColumns(schema) === Seq(
        "agg_state" -> "AggregateFunction(sum, Int32)",
        "location" -> "Point"
      ))
    }
  }

  test("query only supported columns of a distributed table containing unsupported column types") {
    withUnsupportedTypeDistTable("issue_457_cluster_read_db") { (db, tbl) =>
      // spark.clickhouse.read.distributed.convertLocal=true (suite default): reads the local tables
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM $db.$tbl ORDER BY customer_id"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil
      )
      checkAnswer(spark.sql(s"SELECT count(*) FROM $db.$tbl"), Row(4) :: Nil)

      // read through the Distributed table itself
      spark.sql("set spark.clickhouse.read.distributed.convertLocal=false")
      try
        checkAnswer(
          spark.sql(s"SELECT customer_id, client_id FROM $db.$tbl ORDER BY customer_id"),
          Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil
        )
      finally
        spark.sql("set spark.clickhouse.read.distributed.convertLocal=true")
    }
  }

  test("write to supported columns of a distributed table containing unsupported column types") {
    withUnsupportedTypeDistTable("issue_457_cluster_write_db") { (db, tbl) =>
      import testImplicits._
      Seq((5, "e"), (6, "f")).toDF("customer_id", "client_id")
        .writeTo(s"$db.$tbl")
        .append()
      runClickHouseSQL(s"SYSTEM FLUSH DISTRIBUTED $db.$tbl")
      checkAnswer(
        spark.sql(s"SELECT customer_id, client_id FROM $db.$tbl WHERE customer_id > 4 ORDER BY customer_id"),
        Row(5, "e") :: Row(6, "f") :: Nil
      )

      val e = intercept[CHClientException] {
        Seq((7, "g", "x")).toDF("customer_id", "client_id", "agg_state")
          .writeTo(s"$db.$tbl")
          .append()
      }
      assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
    }
  }

  test("SELECT * on a distributed table containing unsupported column types fails") {
    withUnsupportedTypeDistTable("issue_457_cluster_star_db") { (db, tbl) =>
      val e = intercept[CHClientException] {
        spark.sql(s"SELECT * FROM $db.$tbl").collect()
      }
      assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
      assert(e.getMessage.contains("`location` Point"))
    }
  }
}
