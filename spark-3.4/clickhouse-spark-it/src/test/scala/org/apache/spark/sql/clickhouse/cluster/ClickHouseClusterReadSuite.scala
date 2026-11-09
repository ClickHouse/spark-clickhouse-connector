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

import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{READ_DISTRIBUTED_CONVERT_LOCAL, READ_TASK_SPLIT_STRATEGY}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.TASK_SPLIT_STRATEGY_ONE_TASK
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class ClickHouseClusterReadSuite extends SparkClickHouseClusterTest {

  test("clickhouse metadata column - distributed table") {
    withSimpleDistTable("single_replica", "db_w", "t_dist", true) { (_, db, tbl_dist, _) =>
      assert(READ_DISTRIBUTED_CONVERT_LOCAL.defaultValueString == "true")

      withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "true") {
        // `_shard_num` is dedicated for Distributed table
        val cause = intercept[AnalysisException] {
          spark.sql(s"SELECT y, _shard_num FROM $db.$tbl_dist")
        }
        assert(cause.message.contains("`_shard_num` cannot be resolved"))
      }

      withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "false") {
        checkAnswer(
          spark.sql(s"SELECT y, _shard_num FROM $db.$tbl_dist"),
          Seq(
            Row(2021, 2),
            Row(2022, 3),
            Row(2023, 4),
            Row(2024, 1)
          )
        )
      }
    }
  }

  test("push down aggregation - distributed table") {
    withSimpleDistTable("single_replica", "db_agg_col", "t_dist", true) { (_, db, tbl_dist, _) =>
      checkAnswer(
        spark.sql(s"SELECT COUNT(id) FROM $db.$tbl_dist"),
        Seq(Row(4))
      )

      checkAnswer(
        spark.sql(s"SELECT MIN(id) FROM $db.$tbl_dist"),
        Seq(Row(1))
      )

      checkAnswer(
        spark.sql(s"SELECT MAX(id) FROM $db.$tbl_dist"),
        Seq(Row(4))
      )

      checkAnswer(
        spark.sql(s"SELECT m, COUNT(DISTINCT id) FROM $db.$tbl_dist GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 1),
          Row(3, 1),
          Row(4, 1)
        )
      )

      checkAnswer(
        spark.sql(s"SELECT m, SUM(DISTINCT id) FROM $db.$tbl_dist GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 2),
          Row(3, 3),
          Row(4, 4)
        )
      )
    }
  }

  test("push down aggregation - distributed table with cluster macros") {
    withSimpleDistTableUsingMacro("{cluster}", "single_replica", "db_agg_col", "t_dist", true) { (_, db, tbl_dist, _) =>
      checkAnswer(
        spark.sql(s"SELECT COUNT(id) FROM $db.$tbl_dist"),
        Seq(Row(4))
      )

      checkAnswer(
        spark.sql(s"SELECT MIN(id) FROM $db.$tbl_dist"),
        Seq(Row(1))
      )

      checkAnswer(
        spark.sql(s"SELECT MAX(id) FROM $db.$tbl_dist"),
        Seq(Row(4))
      )

      checkAnswer(
        spark.sql(s"SELECT m, COUNT(DISTINCT id) FROM $db.$tbl_dist GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 1),
          Row(3, 1),
          Row(4, 1)
        )
      )

      checkAnswer(
        spark.sql(s"SELECT m, SUM(DISTINCT id) FROM $db.$tbl_dist GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 2),
          Row(3, 3),
          Row(4, 4)
        )
      )
    }
  }

  test("aggregation without GROUP BY and oneTask read a distributed table in one task") {
    withSimpleDistTable("single_replica", "db_one_task", "t_dist", true) { (_, db, tbl_dist, tbl_local) =>
      // convertLocal is on: a plain read is one task per shard and partition of the local table
      val plain = inputPartitions(spark.table(s"$db.$tbl_dist"))
      assert(plain.length > 1 && plain.forall(_.table.name == tbl_local))

      // the tasks for ids 1 and 2 would match nothing and return a MIN of 0, ClickHouse's default for an empty match
      val agg = spark.sql(s"SELECT MIN(id), MAX(id), COUNT(*) FROM $db.$tbl_dist WHERE id >= 3")
      assert(inputPartitions(agg).map(_.table.name) === Seq(tbl_dist))
      checkAnswer(agg, Row(3L, 4L, 2L))

      withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_ONE_TASK) {
        val df = spark.table(s"$db.$tbl_dist")
        assert(inputPartitions(df).map(_.table.name) === Seq(tbl_dist))
        checkAnswer(df.select("id"), Seq(Row(1L), Row(2L), Row(3L), Row(4L)))
      }
    }
  }

  test("runtime filter - distributed table") {
    withSimpleDistTable("single_replica", "runtime_db", "runtime_tbl", true) { (_, db, tbl_dist, _) =>
      spark.sql("set spark.clickhouse.read.runtimeFilter.enabled=false")
      checkAnswer(
        spark.sql(s"SELECT id FROM $db.$tbl_dist " +
          s"WHERE id IN (" +
          s"  SELECT id FROM $db.$tbl_dist " +
          s"  WHERE DATE_FORMAT(create_time, 'yyyy-MM-dd') between '2021-01-01' and '2022-01-01'" +
          s")"),
        Row(1)
      )

      spark.sql("set spark.clickhouse.read.runtimeFilter.enabled=true")
      val df = spark.sql(s"SELECT id FROM $db.$tbl_dist " +
        s"WHERE id IN (" +
        s"  SELECT id FROM $db.$tbl_dist " +
        s"  WHERE DATE_FORMAT(create_time, 'yyyy-MM-dd') between '2021-01-01' and '2022-01-01'" +
        s")")
      checkAnswer(df, Row(1))
      val runtimeFilterExists = df.queryExecution.sparkPlan.exists {
        case BatchScanExec(_, _, runtimeFilters, _, _, table, _, _, _)
            if table.name() == TableIdentifier(tbl_dist, Some(db)).quotedString
              && runtimeFilters.nonEmpty => true
        case _ => false
      }
      assert(runtimeFilterExists)
    }
  }
}
