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
import com.clickhouse.spark.spec.PartitionSpec
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  READ_DISTRIBUTED_CONVERT_LOCAL,
  READ_PARTITION_LISTING_UNION_REPLICAS,
  READ_SPLIT_BY_PARTITION_ID
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

  test("a unioned partition listing counts each part once per replica set") {
    // asserts the union engages; deduplication is covered by the replicated-table tests below,
    // since withSimpleDistTable's locals are non-replicated MergeTree
    withSimpleDistTable("single_replica", "db_listing", "t_dist", true) { (_, db, _, tbl_local) =>
      val df = spark.sql(s"SELECT id FROM $db.$tbl_local")
      val unioned = df.collect().map(_.getLong(0)).sorted
      val scan = df.queryExecution.sparkPlan.collectFirst {
        case b: BatchScanExec => b.scan.asInstanceOf[ClickHouseBatchScan]
      }.get
      // the fixture writes 4 rows across the shards; each lives on 2 replicas
      assert(scan.inputPartitions.map(_.partition.row_count).sum === 4)

      // `default` also spans the other shard, so the listing reports partitions this server does
      // not hold: those tasks must read nothing rather than change the answer
      var ownView: Array[Long] = Array.empty
      withSQLConf(READ_PARTITION_LISTING_UNION_REPLICAS.key -> "false") {
        ownView = spark.sql(s"SELECT id FROM $db.$tbl_local").collect().map(_.getLong(0)).sorted
      }
      assert(unioned === ownView)
    }
  }

  test("a table present only on the connected server reads correctly under the union") {
    // discovery finds cluster `default`, whose other members do not have this table and so
    // contribute nothing to the union. The fallback itself is not covered: it needs an unreachable
    // replica, which CI cannot stage.
    val db = "db_listing_local"
    val tbl = "tbl_node_local"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    try {
      spark.sql(
        s"""CREATE TABLE $db.$tbl (
           |  id BIGINT NOT NULL,
           |  m  INT    NOT NULL
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (order_by = 'id', engine = 'MergeTree()')
           |""".stripMargin
      )
      spark.createDataFrame(Seq((1L, 1), (2L, 2))).toDF("id", "m")
        .writeTo(s"$db.$tbl").append()
      checkAnswer(spark.sql(s"SELECT id FROM $db.$tbl ORDER BY id"), Seq(Row(1L), Row(2L)))
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $db.$tbl")
      spark.sql(s"DROP DATABASE IF EXISTS $db")
    }
  }

  test("a pushed-down aggregate is not corrupted by partitions this server does not hold") {
    // the union reports the other shard's partitions; those tasks match no rows, and a pushed
    // MIN with no GROUP BY returns the empty-set aggregate (0 for a non-nullable column) which
    // Spark would fold into the global result
    withSimpleDistTable("single_replica", "db_agg_union", "t_dist", true) { (_, db, _, tbl_local) =>
      val unioned = spark.sql(s"SELECT MIN(id) FROM $db.$tbl_local").collect()
      var ownView: Array[Row] = Array.empty
      withSQLConf(READ_PARTITION_LISTING_UNION_REPLICAS.key -> "false") {
        ownView = spark.sql(s"SELECT MIN(id) FROM $db.$tbl_local").collect()
      }
      assert(unioned === ownView)
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

  private def plannedPartitions(db: String, tbl: String): Seq[PartitionSpec] =
    spark.sql(s"SELECT id FROM $db.$tbl").queryExecution.sparkPlan.collectFirst {
      case b: BatchScanExec => b.scan.asInstanceOf[ClickHouseBatchScan]
    }.get.inputPartitions.map(_.partition).toSeq

  test("a unioned partition listing counts a replicated part once") {
    withReplicatedTable("db_dedupe", "t_dedupe") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES (1, 1), (2, 2)")
      runClickHouseSQL(s"SYSTEM SYNC REPLICA $db.$tbl", s1r2CmdRunnerOptions)
      // both replicas now report both parts, so without deduplication by name this sums to 4
      assert(plannedPartitions(db, tbl).map(_.row_count).sum === 2)
    }
  }

  test("a unioned partition listing recovers a partition this server has not fetched") {
    withReplicatedTable("db_lag", "t_lag") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES (1, 1)")
      runClickHouseSQL(s"SYSTEM SYNC REPLICA $db.$tbl", s1r2CmdRunnerOptions)
      // stall this server's replication, then write to its peer: the partition exists but is
      // absent from this server's own `system.parts`, which is the bug the union fixes
      runClickHouseSQL(s"SYSTEM STOP FETCHES $db.$tbl")
      try {
        runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES (2, 2)", s1r2CmdRunnerOptions)
        withSQLConf(READ_PARTITION_LISTING_UNION_REPLICAS.key -> "false") {
          assert(!plannedPartitions(db, tbl).exists(_.partition_id == "2"))
        }
        assert(plannedPartitions(db, tbl).exists(_.partition_id == "2"))
        // filtering by partition value instead compares against a value rendered by whichever
        // replica answered, so the listing is left un-unioned — by conf and by per-read option,
        // which is the form the reader actually honours
        withSQLConf(READ_SPLIT_BY_PARTITION_ID.key -> "false") {
          assert(!plannedPartitions(db, tbl).exists(_.partition_id == "2"))
        }
        val byOption = spark.read
          .option(READ_SPLIT_BY_PARTITION_ID.key, "false")
          .table(s"$db.$tbl")
          .queryExecution.sparkPlan.collectFirst {
            case b: BatchScanExec => b.scan.asInstanceOf[ClickHouseBatchScan]
          }.get.inputPartitions
        assert(byOption.forall(!_.filterByPartitionId))
        assert(!byOption.exists(_.partition.partition_id == "2"))
        // the kill switch is honoured in the same two forms
        val unionOff = spark.read
          .option(READ_PARTITION_LISTING_UNION_REPLICAS.key, "false")
          .table(s"$db.$tbl")
          .queryExecution.sparkPlan.collectFirst {
            case b: BatchScanExec => b.scan.asInstanceOf[ClickHouseBatchScan]
          }.get.inputPartitions
        assert(unionOff.forall(_.filterByPartitionId))
        assert(!unionOff.exists(_.partition.partition_id == "2"))
      } finally
        runClickHouseSQL(s"SYSTEM START FETCHES $db.$tbl")
    }
  }
}
