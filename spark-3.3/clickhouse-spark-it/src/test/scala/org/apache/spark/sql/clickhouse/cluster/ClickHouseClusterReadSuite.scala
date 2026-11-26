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

import com.clickhouse.spark.read.ClickHouseBatchScan
import com.clickhouse.spark.spec.ShardSpec
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.READ_DISTRIBUTED_CONVERT_LOCAL
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
        assert(cause.message.contains("cannot resolve '_shard_num' given input columns") ||
          cause.message.contains("Column '_shard_num' does not exist"))
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
        case BatchScanExec(_, _, runtimeFilters, _) if runtimeFilters.nonEmpty => true
        case _ => false
      }
      assert(runtimeFilterExists)
    }
  }

  test("convertLocal read plans per-shard partition id ranges and reads partitions created after planning") {
    withSimpleDistTable("single_replica", "db_part_range", "t_dist", true) { (_, db, tbl_dist, _) =>
      def insert(rows: String): Unit = runClickHouseSQL(
        s"INSERT INTO $db.$tbl_dist (create_time, y, m, id, value) SETTINGS distributed_foreground_insert = 1 " +
          s"VALUES $rows"
      )
      // y=2021 is routed to shard 2 (see the metadata column test), which already holds m=1
      insert("('2021-05-05 00:00:00', 2021, 5, 5, '5'), ('2021-06-06 00:00:00', 2021, 6, 6, '6')")

      withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "true") {
        val df = spark.sql(s"SELECT id FROM $db.$tbl_dist")
        val planned = df.queryExecution.sparkPlan.collectFirst { case s: BatchScanExec => s.scan }
          .getOrElse(fail(s"no ClickHouse scan in ${df.queryExecution.sparkPlan}"))
          .asInstanceOf[ClickHouseBatchScan]
          .inputPartitions
          .toSeq
        val byShard = planned.groupBy(_.candidateNodes.asInstanceOf[ShardSpec].num)
        assert(planned.length === 6)
        assert(byShard.keySet === Set(1, 2, 3, 4))
        byShard.values.foreach { tasks =>
          assert(
            tasks.head.partitionIdRange.exists(_.lower.isEmpty) && tasks.last.partitionIdRange.exists(_.upper.isEmpty)
          )
        }
        assert(byShard(2).map(_.partFilterExpr) === Seq(
          "_partition_id < '5'",
          "_partition_id >= '5' AND _partition_id < '6'",
          "_partition_id >= '6'"
        ))
        Seq(1, 3, 4).foreach(shard => assert(byShard(shard).map(_.partFilterExpr) === Seq("1=1")))

        // after planning: a new partition between listed ids on shard 2, and one on single-partition shard 3
        insert("('2021-03-03 00:00:00', 2021, 3, 7, '7'), ('2022-07-07 00:00:00', 2022, 7, 8, '8')")
        // collect the same Dataset: a new one would list system.parts again
        assert(df.collect().map(_.getLong(0)).sorted.toSeq === (1L to 8L).toList)
      }
    }
  }
}
