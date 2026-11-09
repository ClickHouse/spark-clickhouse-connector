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

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn}
import com.clickhouse.spark.spec.NoPartitionSpec
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{READ_PARALLELISM, READ_TASK_SPLIT_STRATEGY}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  TASK_SPLIT_STRATEGY_ONE_TASK,
  TASK_SPLIT_STRATEGY_PARTITION,
  TASK_SPLIT_STRATEGY_PRIMARY_INDEX
}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.scalatest.tags.Cloud

@Cloud
class ClickHouseCloudPrimaryKeySplitSuite extends ClickHousePrimaryKeySplitSuite with ClickHouseCloudMixIn

class ClickHouseSinglePrimaryKeySplitSuite extends ClickHousePrimaryKeySplitSuite with ClickHouseSingleMixIn

abstract class ClickHousePrimaryKeySplitSuite extends SparkClickHouseSingleTest {

  private val granule = 64
  private val rows = 9000

  test("a read is split into ranges of the primary key holding about as many rows each") {
    withIdTable() { (db, tbl) =>
      withSQLConf(READ_PARALLELISM.key -> "4") {
        eventuallyOnCloud {
          val df = spark.table(s"$db.$tbl")
          val parts = inputPartitions(df)
          assert(parts.length === 4)
          assert(parts.forall(part => part.keyRange.isDefined && part.partition == NoPartitionSpec))
          rowsPerTask(df).foreach(n => assert(math.abs(n - rows / 4) <= granule))
        }
        checkAnswer(spark.table(s"$db.$tbl"), (0 until rows).map(id => Row(id.toLong, id / (rows / 3))))
      }
    }
  }

  test("every range of a filtered read holds rows the query reads") {
    withIdTable() { (db, tbl) =>
      withSQLConf(READ_PARALLELISM.key -> "4") {
        // only the last ClickHouse partition can match
        val lastThird = s"id >= ${rows - rows / 3}"
        eventuallyOnCloud {
          val df = spark.table(s"$db.$tbl").filter(lastThird)
          assert(inputPartitions(df).length === 4)
          rowsPerTask(df).foreach(n => assert(math.abs(n - rows / 3 / 4) <= granule))
        }
        val expected = (rows - rows / 3 until rows).map(id => Row(id.toLong))
        checkAnswer(spark.table(s"$db.$tbl").filter(lastThird).select("id"), expected)
      }
    }
  }

  test("a read falls back to one task when the primary key can't be split on") {
    // NaN falls outside every range, so a Float64 key is never split on
    withTable(
      "db_pk_split",
      "tbl_pk_split_float",
      StructType(Seq(StructField("f", DoubleType, nullable = false), StructField("m", IntegerType, nullable = false))),
      sortKeys = Seq("f"),
      partKeys = Seq("m")
    ) { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` SELECT number / 2, intDiv(number, ${rows / 3}) FROM numbers($rows)")
      val parts = inputPartitions(spark.table(s"$db.$tbl"))
      assert(parts.length === 1)
      assert(parts.head.keyRange.isEmpty && parts.head.partition == NoPartitionSpec)
      checkAnswer(spark.table(s"$db.$tbl").select("m"), (0 until rows).map(id => Row(id / (rows / 3))))
      withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_PARTITION) {
        eventuallyOnCloud(assert(inputPartitions(spark.table(s"$db.$tbl")).length === 3))
      }
    }
  }

  test("a read is one task under the oneTask task split strategy") {
    withIdTable() { (db, tbl) =>
      withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_ONE_TASK) {
        val df = spark.table(s"$db.$tbl")
        val parts = inputPartitions(df)
        assert(parts.length === 1)
        assert(parts.head.keyRange.isEmpty && parts.head.partition == NoPartitionSpec)
        checkAnswer(df.select("id"), (0 until rows).map(id => Row(id.toLong)))
      }
    }
  }

  test("a read is split by ClickHouse partition under the partitionSplit task split strategy") {
    withIdTable() { (db, tbl) =>
      withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> TASK_SPLIT_STRATEGY_PARTITION, READ_PARALLELISM.key -> "4") {
        val df = spark.table(s"$db.$tbl")
        eventuallyOnCloud {
          val parts = inputPartitions(spark.table(s"$db.$tbl"))
          assert(parts.length === 3)
          assert(parts.forall(_.keyRange.isEmpty))
        }
        checkAnswer(df.select("id"), (0 until rows).map(id => Row(id.toLong)))
      }
    }
  }

  test("the parallelism can be set per read") {
    withIdTable() { (db, tbl) =>
      eventuallyOnCloud {
        val parts = inputPartitions(spark.read.option(READ_PARALLELISM.key, "5").table(s"$db.$tbl"))
        assert(parts.length === 5)
        assert(parts.forall(_.keyRange.isDefined))
      }
    }
  }

  test("the task split strategy is set per read in any case and must be a known one") {
    withIdTable() { (db, tbl) =>
      eventuallyOnCloud {
        val parts = inputPartitions(
          spark.read.option(READ_TASK_SPLIT_STRATEGY.key, TASK_SPLIT_STRATEGY_PARTITION.toUpperCase).table(s"$db.$tbl")
        )
        assert(parts.length === 3)
        assert(parts.forall(_.keyRange.isEmpty))
      }
      val e = intercept[IllegalArgumentException] {
        inputPartitions(spark.read.option(READ_TASK_SPLIT_STRATEGY.key, "rangeSplit").table(s"$db.$tbl"))
      }
      assert(e.getMessage.contains(READ_TASK_SPLIT_STRATEGY.key))
      assert(e.getMessage.contains(TASK_SPLIT_STRATEGY_PRIMARY_INDEX))
    }
  }

  test("an aggregation with GROUP BY is pushed into each range") {
    withIdTable() { (db, tbl) =>
      withSQLConf(READ_PARALLELISM.key -> "4") {
        val sql = s"SELECT m, COUNT(*), MIN(id), MAX(id), SUM(id) FROM $db.$tbl WHERE id >= 100 GROUP BY m"
        eventuallyOnCloud(assert(inputPartitions(spark.sql(sql)).length === 4))
        val expected = (100 until rows).groupBy(_ / (rows / 3)).toSeq.map { case (m, ids) =>
          Row(m, ids.length.toLong, ids.min.toLong, ids.max.toLong, ids.map(_.toLong).sum)
        }
        checkAnswer(spark.sql(sql), expected)
      }
    }
  }

  test("an aggregation without GROUP BY is read as one task under every strategy") {
    // a key range or partition the filter empties still returns a row of defaults (a MIN of 0, not NULL)
    // that Spark would merge into the result
    withIdTable() { (db, tbl) =>
      Seq(TASK_SPLIT_STRATEGY_PRIMARY_INDEX, TASK_SPLIT_STRATEGY_PARTITION).foreach { strategy =>
        withSQLConf(READ_TASK_SPLIT_STRATEGY.key -> strategy, READ_PARALLELISM.key -> "4") {
          val sql = s"SELECT MIN(id), MAX(id), COUNT(*) FROM $db.$tbl WHERE m = 0 AND id >= 100"
          assert(inputPartitions(spark.sql(sql)).length === 1)
          checkAnswer(spark.sql(sql), Row(100L, rows / 3 - 1L, rows / 3 - 100L))
        }
      }
    }
  }

  /** Creates a table of `rows` ids in 64-row granules, partitioned into thirds of the ids. */
  private def withIdTable()(f: (String, String) => Unit): Unit =
    withTable(
      "db_pk_split",
      "tbl_pk_split",
      StructType(Seq(StructField("id", LongType, nullable = false), StructField("m", IntegerType, nullable = false))),
      partKeys = Seq("m"),
      extraProperties = Map("settings.index_granularity" -> granule.toString)
    ) { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` SELECT number, intDiv(number, ${rows / 3}) FROM numbers($rows)")
      f(db, tbl)
    }

  private def rowsPerTask(df: DataFrame): Seq[Int] = df.rdd.mapPartitions(it => Iterator(it.size)).collect().toSeq
}
