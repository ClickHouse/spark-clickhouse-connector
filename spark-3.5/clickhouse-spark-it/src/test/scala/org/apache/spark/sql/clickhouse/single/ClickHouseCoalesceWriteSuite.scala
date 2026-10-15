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
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

/**
 * End-to-end integration tests for the arrow write coalescing path
 * (`spark.clickhouse.write.coalesce.enabled`).
 *
 * Test 1 proves a coalesced arrow write produces correct data.
 * Test 2 proves the disabled (default) path still writes correctly.
 */
class ClickHouseCoalesceWriteSuite extends SparkClickHouseSingleTest with ClickHouseSingleMixIn {

  /** Set the given spark.conf keys, run f, then restore the previous values. */
  private def withConf(kvs: (String, String)*)(f: => Unit): Unit = {
    val previous = kvs.map { case (k, _) =>
      k -> (if (spark.conf.contains(k)) Some(spark.conf.get(k)) else None)
    }
    try {
      kvs.foreach { case (k, v) => spark.conf.set(k, v) }
      f
    } finally
      previous.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
  }

  /** Run a single-value ClickHouse query of the form `SELECT ... AS c` and return c. */
  private def clickHouseCount(sql: String): Long = {
    // ClickHouseCommandRunner returns each result row as a single JSON string column, e.g. {"c":"4"}.
    val json = runClickHouseSQL(sql).collect()(0).getString(0)
    """"c"\s*:\s*"?(\d+)"?""".r.findFirstMatchIn(json)
      .map(_.group(1).toLong)
      .getOrElse(throw new AssertionError(s"could not parse count from: $json"))
  }

  /** Completed INSERT queries into db.tbl so far (flushes query_log first). */
  private def insertQueries(db: String, tbl: String): Long = {
    runClickHouseSQL("SYSTEM FLUSH LOGS")
    clickHouseCount(
      s"SELECT count() AS c FROM system.query_log WHERE type = 'QueryFinish' " +
        s"AND query_kind = 'Insert' AND has(tables, '$db.$tbl')"
    )
  }

  // Large target + linger so only task commits trigger coalesced flushes; 1000-row batches.
  private val coalesceOn = Seq(
    "spark.clickhouse.write.format" -> "arrow",
    "spark.clickhouse.write.coalesce.enabled" -> "true",
    "spark.clickhouse.write.coalesce.targetBytes" -> "64m",
    "spark.clickhouse.write.coalesce.lingerMs" -> "60000",
    "spark.clickhouse.write.batchSize" -> "1000"
  )
  private val coalesceOff = coalesceOn.map {
    case ("spark.clickhouse.write.coalesce.enabled", _) => "spark.clickhouse.write.coalesce.enabled" -> "false"
    case kv => kv
  }

  private val idValue = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("v", StringType, nullable = false)
  ))

  private val rows = 40000
  private val tasks = 8

  /** Append `rows` rows over `tasks` tasks and return the number of INSERT queries it took. */
  private def appendAndCountInserts(db: String, tbl: String): Long = {
    val before = insertQueries(db, tbl)
    spark.createDataFrame(spark.sparkContext.parallelize((1 to rows).map(i => Row(i, s"v$i"))), idValue)
      .repartition(tasks)
      .write.mode(SaveMode.Append).saveAsTable(s"$db.$tbl")
    insertQueries(db, tbl) - before
  }

  test("coalesced arrow write is correct") {
    withConf(
      "spark.clickhouse.write.format" -> "arrow",
      "spark.clickhouse.write.coalesce.enabled" -> "true",
      "spark.clickhouse.write.coalesce.targetBytes" -> "64m",
      "spark.clickhouse.write.coalesce.lingerMs" -> "200",
      "spark.clickhouse.write.batchSize" -> "1000"
    ) {
      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("v", StringType, nullable = false)
      ))

      withTable("test_db", "test_coalesce_arrow", schema) { (actualDb: String, actualTbl: String) =>
        val n = 5000
        val data = (1 to n).map(i => Row(i, s"v$i"))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
          .repartition(4)
        df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.$actualTbl")

        val agg = spark.table(s"$actualDb.$actualTbl")
          .selectExpr("count(*) AS c", "sum(id) AS s")
          .collect()(0)
        val count = agg.getLong(0)
        val idSum = agg.getLong(1)
        assert(count == n, s"expected $n rows, got $count")
        assert(idSum == n.toLong * (n + 1) / 2, s"unexpected sum(id)=$idSum")

        // ClickHouseCommandRunner returns each result row as a single JSON string
        // column, e.g. {"c":"4"}. Extract the count() value from it.
        val partsJson = runClickHouseSQL(
          s"SELECT count() AS c FROM system.parts " +
            s"WHERE database='$actualDb' AND table='$actualTbl' AND active"
        ).collect()(0).getString(0)
        val activeParts =
          """"c"\s*:\s*"?(\d+)"?""".r.findFirstMatchIn(partsJson)
            .map(_.group(1).toLong)
            .getOrElse(throw new AssertionError(s"could not parse parts count from: $partsJson"))
        info(s"coalesced arrow write: $count rows, active parts = $activeParts")
        assert(activeParts >= 1, s"expected at least 1 active part, got $activeParts")
      }
    }
  }

  test("coalesce disabled writes correctly (baseline)") {
    withConf(
      "spark.clickhouse.write.format" -> "arrow",
      "spark.clickhouse.write.coalesce.enabled" -> "false"
    ) {
      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)
      ))

      withTable("test_db", "test_coalesce_disabled", schema) { (actualDb: String, actualTbl: String) =>
        val n = 1000
        val data = (1 to n).map(i => Row(i))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
          .repartition(4)
        df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.$actualTbl")

        val count = spark.table(s"$actualDb.$actualTbl").count()
        assert(count == n, s"expected $n rows, got $count")
      }
    }
  }

  test("coalescing sends fewer inserts than the direct path") {
    var direct = -1L
    var coalesced = -1L
    withConf(coalesceOff: _*) {
      withTable("test_db", "test_coalesce_direct_count", idValue) { (db: String, tbl: String) =>
        direct = appendAndCountInserts(db, tbl)
      }
    }
    withConf(coalesceOn: _*) {
      withTable("test_db", "test_coalesce_count", idValue) { (db: String, tbl: String) =>
        coalesced = appendAndCountInserts(db, tbl)
        assert(spark.table(s"$db.$tbl").count() == rows)
      }
    }
    info(s"inserts for $rows rows / $tasks tasks: direct = $direct, coalesced = $coalesced")
    assert(direct == rows / 1000, s"direct path should send one insert per batch, got $direct")
    assert(coalesced <= tasks, s"coalesced inserts ($coalesced) should not exceed one per task commit")
  }

  test("repeated coalesced writes to the same table keep coalescing") {
    withConf(coalesceOn: _*) {
      withTable("test_db", "test_coalesce_repeat", idValue) { (db: String, tbl: String) =>
        val perJob = (1 to 3).map(_ => appendAndCountInserts(db, tbl))
        info(s"inserts per job: ${perJob.mkString(", ")}")
        perJob.zipWithIndex.foreach { case (n, i) =>
          assert(n <= tasks, s"job ${i + 1} sent $n inserts; coalescing stopped after an earlier job")
        }
        assert(spark.table(s"$db.$tbl").count() == 3L * rows)
      }
    }
  }

  test("coalesced write after the table is recreated with a different schema writes correct data") {
    withConf(coalesceOn: _*) {
      withTable("test_db", "test_coalesce_recreate", idValue) { (db: String, tbl: String) =>
        appendAndCountInserts(db, tbl)
      }
      val wider = StructType(Seq(
        StructField("id", LongType, nullable = false),
        StructField("b", DoubleType, nullable = false),
        StructField("c", StringType, nullable = false)
      ))
      withTable("test_db", "test_coalesce_recreate", wider) { (db: String, tbl: String) =>
        val n = 2000
        spark.createDataFrame(
          spark.sparkContext.parallelize((1 to n).map(i => Row(i.toLong, i * 1.5, s"c$i"))),
          wider
        ).repartition(4).write.mode(SaveMode.Append).saveAsTable(s"$db.$tbl")

        val agg = spark.table(s"$db.$tbl")
          .selectExpr("count(*)", "sum(id)", "sum(b)", "sum(cast(c = '' AS INT))")
          .collect()(0)
        assert(agg.getLong(0) == n)
        assert(agg.getLong(1) == n.toLong * (n + 1) / 2, s"sum(id) = ${agg.getLong(1)}")
        assert(agg.getDouble(2) == 1.5 * n * (n + 1) / 2, s"sum(b) = ${agg.getDouble(2)}")
        assert(agg.getLong(3) == 0, s"${agg.getLong(3)} rows have an empty c")
      }
    }
  }
}
