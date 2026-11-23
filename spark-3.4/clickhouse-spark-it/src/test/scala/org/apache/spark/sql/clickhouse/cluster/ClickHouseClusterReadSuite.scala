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

import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  READ_DISTRIBUTED_CONVERT_LOCAL,
  READ_PRIMARY_INDEX_SPLIT_MAX_PARALLELISM,
  READ_PRIMARY_INDEX_SPLIT_TARGET_ROWS_PER_TASK,
  READ_TASK_SPLIT_STRATEGY
}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  TASK_SPLIT_STRATEGY_ONE_TASK,
  TASK_SPLIT_STRATEGY_PRIMARY_INDEX
}
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

  test("a distributed table is split shard by shard with convertLocal, and read in one task without") {
    withSimpleDistTable("single_replica", "db_one_task", "t_dist", true) { (_, db, tbl_dist, tbl_local) =>
      // convertLocal is on; partitionSplit, the default, reads one task per shard and partition of the local table
      val parts = inputPartitions(spark.table(s"$db.$tbl_dist"))
      assert(parts.length > 1 && parts.forall(_.table.name == tbl_local))
      checkAnswer(spark.table(s"$db.$tbl_dist").select("id"), Seq(Row(1L), Row(2L), Row(3L), Row(4L)))

      // the tasks for ids 1 and 2 would match nothing and return a MIN of 0, ClickHouse's default for an empty match
      val agg = spark.sql(s"SELECT MIN(id), MAX(id), COUNT(*) FROM $db.$tbl_dist WHERE id >= 3")
      assert(inputPartitions(agg).map(_.table.name) === Seq(tbl_dist))
      checkAnswer(agg, Row(3L, 4L, 2L))

      // without it the partitions can't be listed through the engine, and the read is one task
      withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "false") {
        assert(inputPartitions(spark.table(s"$db.$tbl_dist")).map(_.table.name) === Seq(tbl_dist))
      }

      withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_PRIMARY_INDEX) {
        // a shard holds one row, too few to cut, so primaryIndexSplit reads it in a task
        val byShard = inputPartitions(spark.table(s"$db.$tbl_dist"))
        assert(byShard.length === 4 && byShard.forall(p => p.table.name == tbl_local && p.filterExpr == "1=1"))
        checkAnswer(spark.table(s"$db.$tbl_dist").select("id"), Seq(Row(1L), Row(2L), Row(3L), Row(4L)))

        // without convertLocal there is no index across shards, and the read is one task through the engine
        withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "false") {
          assert(inputPartitions(spark.table(s"$db.$tbl_dist")).map(_.table.name) === Seq(tbl_dist))
        }
      }

      withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_ONE_TASK) {
        val df = spark.table(s"$db.$tbl_dist")
        assert(inputPartitions(df).map(_.table.name) === Seq(tbl_dist))
        checkAnswer(df.select("id"), Seq(Row(1L), Row(2L), Row(3L), Row(4L)))
      }
    }
  }

  test("primaryIndexSplit cuts every shard's local table into ranges of its primary key") {
    val (rows, granule) = (8000, 64)
    autoCleanupDistTable("single_replica", "db_key_split", "t_dist") { (cluster, db, tbl_dist, tbl_local) =>
      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl_local ON CLUSTER '$cluster' (id Int64)
           |ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = $granule
           |""".stripMargin
      )
      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl_dist ON CLUSTER '$cluster' (id Int64)
           |ENGINE = Distributed('$cluster', '$db', '$tbl_local', id)
           |""".stripMargin
      )
      runClickHouseSQL(
        s"INSERT INTO $db.$tbl_dist SETTINGS insert_distributed_sync = 1 SELECT number FROM numbers($rows)"
      )
      withSQLConf(
        READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_PRIMARY_INDEX,
        READ_PRIMARY_INDEX_SPLIT_TARGET_ROWS_PER_TASK.key -> "500"
      ) {
        // every shard is cut by its own rows: 2000 in tasks of 500
        val bySize = inputPartitions(spark.table(s"$db.$tbl_dist"))
        assert(bySize.length === 16)
        assert(bySize.groupBy(_.candidateNodes.nodes.head).values.forall(_.length == 4))
        // 4 shards share a maximum of 8 tasks: 2 key ranges of its local table each
        withSQLConf(READ_PRIMARY_INDEX_SPLIT_MAX_PARALLELISM.key -> "8") {
          val df = spark.table(s"$db.$tbl_dist")
          val parts = inputPartitions(df)
          assert(parts.length === 8)
          assert(parts.forall(p => p.table.name == tbl_local && p.filterExpr.startsWith("(`id` ")))
          assert(parts.groupBy(_.candidateNodes.nodes.head).values.forall(_.length == 2))
          df.rdd.mapPartitions(it => Iterator(it.size)).collect()
            .foreach(n => assert(math.abs(n - rows / 8) <= granule))
          checkAnswer(df, (0 until rows).map(id => Row(id.toLong)))
        }
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
