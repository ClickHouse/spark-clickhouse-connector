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

class ClickHouseSingleTableProviderSuite extends ClickHouseTableProviderSuite with ClickHouseSingleMixIn

/**
 * Test suite for ClickHouse DataSource V2 TableProvider write capabilities.
 * Tests format API, write modes, predicate pushdown, and partition overwrite.
 */
abstract class ClickHouseTableProviderSuite extends SparkClickHouseSingleTest {

  import testImplicits._

  private val testSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false),
    StructField("created", TimestampType, nullable = false)
  ))

  test("write and read using format API") {
    withTable("format_api_db", "test_format_api", testSchema) {
      val writeData = Seq(
        (1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")),
        (2, "Bob", 87.3, timestamp("2024-02-20T14:30:00Z")),
        (3, "Charlie", 92.1, timestamp("2024-03-10T09:15:00Z"))
      ).toDF("id", "name", "value", "created")

      writeData.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .options(cmdRunnerOptions)
        .option("database", if (useSuiteLevelDatabase) testDatabaseName else "format_api_db")
        .option("table", "test_format_api")
        .save()

      val readData = spark.read
        .format("clickhouse")
        .options(cmdRunnerOptions)
        .option("database", if (useSuiteLevelDatabase) testDatabaseName else "format_api_db")
        .option("table", "test_format_api")
        .load()

      // Verify schema
      assert(readData.schema.length == 4)
      assert(readData.schema.fieldNames.contains("id"))
      assert(readData.schema.fieldNames.contains("name"))

      // Verify data
      checkAnswer(
        readData.orderBy("id"),
        writeData.orderBy("id")
      )

      // Verify count
      assert(readData.count() == 3)
    }
  }

  test("append mode adds data without replacing existing") {
    withTable("append_db", "test_append", testSchema) {
      val initialData = Seq(
        (1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")),
        (2, "Bob", 87.3, timestamp("2024-02-20T14:30:00Z"))
      ).toDF("id", "name", "value", "created")

      val options = cmdRunnerOptions ++
        Map("database" -> (if (useSuiteLevelDatabase) testDatabaseName else "append_db"), "table" -> "test_append")

      initialData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val appendData = Seq(
        (3, "Charlie", 92.1, timestamp("2024-03-10T09:15:00Z")),
        (4, "David", 88.7, timestamp("2024-04-05T11:20:00Z"))
      ).toDF("id", "name", "value", "created")

      appendData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val result = spark.read.format("clickhouse").options(options).load()

      assert(result.count() == 4)
      checkAnswer(
        result.filter("id = 1"),
        Row(1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")) :: Nil
      )
      checkAnswer(
        result.filter("id = 4"),
        Row(4, "David", 88.7, timestamp("2024-04-05T11:20:00Z")) :: Nil
      )
    }
  }

  test("overwrite mode replaces all existing data") {
    withTable("overwrite_db", "test_overwrite", testSchema) {
      val initialData = Seq(
        (1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")),
        (2, "Bob", 87.3, timestamp("2024-02-20T14:30:00Z")),
        (3, "Charlie", 92.1, timestamp("2024-03-10T09:15:00Z"))
      ).toDF("id", "name", "value", "created")

      val options = cmdRunnerOptions ++
        Map(
          "database" -> (if (useSuiteLevelDatabase) testDatabaseName else "overwrite_db"),
          "table" -> "test_overwrite"
        )

      // Write initial data
      initialData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      // Verify initial data
      val initialResult = spark.read.format("clickhouse").options(options).load()
      assert(initialResult.count() == 3, "Should have 3 rows initially")
      checkAnswer(
        initialResult.filter("id = 1"),
        Row(1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")) :: Nil
      )

      // Overwrite with new data
      val newData = Seq(
        (4, "David", 88.7, timestamp("2024-04-05T11:20:00Z")),
        (5, "Eve", 91.2, timestamp("2024-05-12T16:45:00Z"))
      ).toDF("id", "name", "value", "created")

      newData.write.format("clickhouse").mode(SaveMode.Overwrite).options(options).save()

      // Verify only new data exists
      val finalResult = spark.read.format("clickhouse").options(options).load()
      assert(finalResult.count() == 2, "Should have only 2 rows after overwrite")

      // Old data should be gone
      assert(finalResult.filter("id = 1").count() == 0, "Old data (id=1) should be removed")
      assert(finalResult.filter("id = 2").count() == 0, "Old data (id=2) should be removed")
      assert(finalResult.filter("id = 3").count() == 0, "Old data (id=3) should be removed")

      // New data should exist
      checkAnswer(
        finalResult.orderBy("id"),
        Seq(
          Row(4, "David", 88.7, timestamp("2024-04-05T11:20:00Z")),
          Row(5, "Eve", 91.2, timestamp("2024-05-12T16:45:00Z"))
        )
      )
    }
  }

  test("schema inference works correctly") {
    val dbName = if (useSuiteLevelDatabase) testDatabaseName else "schema_db"
    val tableName = "test_schema"

    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$dbName`")
      }

      val writeData = Seq(
        (1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z"))
      ).toDF("id", "name", "value", "created")

      val options = cmdRunnerOptions ++
        Map("database" -> dbName, "table" -> tableName, "order_by" -> "id")

      // Let the TableProvider create the table from the DataFrame schema
      writeData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      // Read back and verify schema inference
      val inferredDF = spark.read.format("clickhouse").options(options).load()

      assert(inferredDF.schema("id").dataType == IntegerType)
      assert(inferredDF.schema("name").dataType == StringType)
      assert(inferredDF.schema("value").dataType == DoubleType)
      assert(inferredDF.schema("created").dataType == TimestampType)

      // Verify table was actually created
      assert(inferredDF.count() == 1, "Should have 1 row")
    } finally {
      dropTableWithRetry(dbName, tableName)
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$dbName`")
      }
    }
  }

  test("predicate pushdown filters data server-side") {
    withTable("filter_db", "test_filter", testSchema) {
      val writeData = Seq(
        (1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")),
        (2, "Bob", 87.3, timestamp("2024-02-20T14:30:00Z")),
        (3, "Charlie", 92.1, timestamp("2024-03-10T09:15:00Z")),
        (4, "David", 88.7, timestamp("2024-04-05T11:20:00Z")),
        (5, "Eve", 91.2, timestamp("2024-05-12T16:45:00Z"))
      ).toDF("id", "name", "value", "created")

      val options = cmdRunnerOptions ++
        Map("database" -> (if (useSuiteLevelDatabase) testDatabaseName else "filter_db"), "table" -> "test_filter")

      writeData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val filtered1 = spark.read.format("clickhouse").options(options).load().filter("value > 90")
      val result1 = filtered1.collect() // Force execution

      runClickHouseSQL("SYSTEM FLUSH LOGS").collect()

      val dbName = if (useSuiteLevelDatabase) testDatabaseName else "filter_db"
      val recentQueriesDF = runClickHouseSQL(
        s"""SELECT query FROM system.query_log 
           |WHERE type = 'QueryFinish' 
           |  AND query LIKE '%$dbName%test_filter%'
           |  AND query LIKE '%SELECT%'
           |  AND event_time > now() - INTERVAL 10 SECOND
           |ORDER BY event_time DESC
           |LIMIT 5""".stripMargin
      )

      val recentQueries = recentQueriesDF.collect().map(_.getString(0))

      val hasFilter = recentQueries.exists(query =>
        query.contains("WHERE") && query.contains("`value` > 90")
      )
      assert(
        hasFilter,
        s"Query should contain WHERE clause with 'value > 90' filter. Recent queries: ${recentQueries.mkString("; ")}"
      )

      assert(result1.length == 3)
      checkAnswer(
        filtered1.select("id").orderBy("id"),
        Seq(Row(1), Row(3), Row(5))
      )

      val filtered2 = spark.read.format("clickhouse").options(options).load().filter("name = 'Bob'")
      val result2 = filtered2.collect()

      runClickHouseSQL("SYSTEM FLUSH LOGS").collect()

      val recentQueries2DF = runClickHouseSQL(
        s"""SELECT query FROM system.query_log 
           |WHERE type = 'QueryFinish' 
           |  AND query LIKE '%$dbName%test_filter%'
           |  AND query LIKE '%SELECT%'
           |  AND event_time > now() - INTERVAL 10 SECOND
           |ORDER BY event_time DESC
           |LIMIT 5""".stripMargin
      )

      val recentQueries2 = recentQueries2DF.collect().map(_.getString(0))

      val hasNameFilter = recentQueries2.exists(query =>
        query.contains("WHERE") && query.contains("`name` = 'Bob'")
      )
      assert(
        hasNameFilter,
        s"Query should contain WHERE clause with 'name = Bob' filter. Recent queries: ${recentQueries2.mkString("; ")}"
      )

      assert(result2.length == 1)
      checkAnswer(filtered2, Row(2, "Bob", 87.3, timestamp("2024-02-20T14:30:00Z")) :: Nil)

      val filtered3 = spark.read.format("clickhouse").options(options).load()
        .filter("value > 90 AND id < 5")
      val result3 = filtered3.collect()

      runClickHouseSQL("SYSTEM FLUSH LOGS").collect()

      val recentQueries3DF = runClickHouseSQL(
        s"""SELECT query FROM system.query_log 
           |WHERE type = 'QueryFinish' 
           |  AND query LIKE '%$dbName%test_filter%'
           |  AND query LIKE '%SELECT%'
           |  AND event_time > now() - INTERVAL 10 SECOND
           |ORDER BY event_time DESC
           |LIMIT 5""".stripMargin
      )

      val recentQueries3 = recentQueries3DF.collect().map(_.getString(0))

      val hasCombinedFilter = recentQueries3.exists(query =>
        query.contains("WHERE") && query.contains("`value` > 90") && query.contains("`id` < 5")
      )
      assert(
        hasCombinedFilter,
        s"Query should contain WHERE clause with 'value > 90 AND id < 5' filters. Recent queries: ${recentQueries3.mkString("; ")}"
      )

      assert(result3.length == 2)
      checkAnswer(
        filtered3.select("name").orderBy("id"),
        Seq(Row("Alice"), Row("Charlie"))
      )
    }
  }

  test("column pruning reduces data transfer") {
    withTable("prune_db", "test_prune", testSchema) {
      val writeData = Seq(
        (1, "Alice", 95.5, timestamp("2024-01-15T10:00:00Z")),
        (2, "Bob", 87.3, timestamp("2024-02-20T14:30:00Z"))
      ).toDF("id", "name", "value", "created")

      val options = cmdRunnerOptions ++
        Map("database" -> (if (useSuiteLevelDatabase) testDatabaseName else "prune_db"), "table" -> "test_prune")

      writeData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val selected = spark.read.format("clickhouse").options(options).load().select("id", "name")
      selected.show()

      runClickHouseSQL("SYSTEM FLUSH LOGS").collect()

      val dbName = if (useSuiteLevelDatabase) testDatabaseName else "prune_db"
      val recentQueriesDF = runClickHouseSQL(
        s"""SELECT query FROM system.query_log 
           |WHERE type = 'QueryFinish' 
           |  AND query LIKE '%$dbName%test_prune%'
           |  AND query LIKE '%SELECT%'
           |  AND event_time > now() - INTERVAL 10 SECOND
           |ORDER BY event_time DESC
           |LIMIT 5""".stripMargin
      )

      val recentQueries = recentQueriesDF.collect().map(_.getString(0))

      // Check that the query only selects id and name columns, not value or created
      val hasPrunedColumns = recentQueries.exists(query =>
        query.contains("SELECT") &&
          query.contains("`id`") &&
          query.contains("`name`") &&
          !query.contains("`value`") &&
          !query.contains("`created`")
      )
      assert(
        hasPrunedColumns,
        s"Query should only SELECT id and name columns, not value or created. Recent queries: ${recentQueries.mkString("; ")}"
      )

      assert(selected.schema.length == 2)
      assert(selected.schema.fieldNames.contains("id"))
      assert(selected.schema.fieldNames.contains("name"))
      assert(!selected.schema.fieldNames.contains("value"))
      assert(!selected.schema.fieldNames.contains("created"))

      checkAnswer(
        selected.orderBy("id"),
        Seq(Row(1, "Alice"), Row(2, "Bob"))
      )
    }
  }

  test("write with null values") {
    val nullableSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)
    ))

    withTable("null_db", "test_nulls", nullableSchema) {
      val writeData = Seq(
        (1, Some("Alice"), Some(95.5)),
        (2, None, Some(87.3)),
        (3, Some("Charlie"), None),
        (4, None, None)
      ).toDF("id", "name", "value")

      val options = cmdRunnerOptions ++
        Map("database" -> (if (useSuiteLevelDatabase) testDatabaseName else "null_db"), "table" -> "test_nulls")

      writeData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val result = spark.read.format("clickhouse").options(options).load()
      assert(result.count() == 4)

      // Verify null values preserved
      val row2 = result.filter("id = 2").collect()(0)
      assert(row2.isNullAt(1), "name should be null")
      assert(!row2.isNullAt(2), "value should not be null")

      val row3 = result.filter("id = 3").collect()(0)
      assert(!row3.isNullAt(1), "name should not be null")
      assert(row3.isNullAt(2), "value should be null")

      val row4 = result.filter("id = 4").collect()(0)
      assert(row4.isNullAt(1), "name should be null")
      assert(row4.isNullAt(2), "value should be null")
    }
  }

  test("write empty dataframe") {
    withTable("empty_db", "test_empty", testSchema) {
      // Create empty DataFrame with proper schema
      val emptyData = Seq.empty[(Int, String, Double, java.sql.Timestamp)]
        .toDF("id", "name", "value", "created")

      val options = cmdRunnerOptions ++
        Map("database" -> (if (useSuiteLevelDatabase) testDatabaseName else "empty_db"), "table" -> "test_empty")

      // Should not fail
      emptyData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val result = spark.read.format("clickhouse").options(options).load()
      assert(result.count() == 0, "Table should be empty")
    }
  }

  test("order by with nullable columns using allow_nullable_key setting") {
    val dbName = if (useSuiteLevelDatabase) testDatabaseName else "ordered_db"
    val tableName = "test_ordered_nullable"

    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$dbName`")
      }

      val writeData = Seq(
        (Some(3), Some("C"), 92.1, timestamp("2024-03-10T09:15:00Z")),
        (Some(1), Some("A"), 95.5, timestamp("2024-01-15T10:00:00Z")),
        (None, Some("B"), 87.3, timestamp("2024-02-20T14:30:00Z")),
        (Some(2), None, 88.7, timestamp("2024-04-05T11:20:00Z"))
      ).toDF("id", "category", "value", "created")

      val options = cmdRunnerOptions ++
        Map(
          "database" -> dbName,
          "table" -> tableName,
          "order_by" -> "id, category",
          "settings.allow_nullable_key" -> "1"
        )

      // This should succeed because allow_nullable_key is enabled
      writeData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()

      val result = spark.read.format("clickhouse").options(options).load()
      assert(result.count() == 4)

      // Verify all data is present
      checkAnswer(
        result.filter("id = 1"),
        Row(1, "A", 95.5, timestamp("2024-01-15T10:00:00Z")) :: Nil
      )

      // Verify null values are preserved
      val nullIdRow = result.filter("id IS NULL").collect()
      assert(nullIdRow.length == 1, "Should have one row with null id")
      assert(nullIdRow(0).isNullAt(0), "id should be null")
      assert(nullIdRow(0).getString(1) == "B", "category should be B")

      val nullCategoryRow = result.filter("category IS NULL").collect()
      assert(nullCategoryRow.length == 1, "Should have one row with null category")
      assert(nullCategoryRow(0).getInt(0) == 2, "id should be 2")
      assert(nullCategoryRow(0).isNullAt(1), "category should be null")
    } finally {
      dropTableWithRetry(dbName, tableName)
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$dbName`")
      }
    }
  }

  test("order by without allow_nullable_key setting should fail on nullable columns") {
    val dbName = if (useSuiteLevelDatabase) testDatabaseName else "fail_ordered_db"
    val tableName = "test_fail_ordered"

    try {
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$dbName`")
      }

      val writeData = Seq(
        (Some(1), "Alice", 95.5),
        (Some(2), "Bob", 87.3)
      ).toDF("id", "name", "value")

      val options = cmdRunnerOptions ++
        Map(
          "database" -> dbName,
          "table" -> tableName,
          "order_by" -> "id",
          "settings.allow_nullable_key" -> "0"
        )

      // This should fail because ORDER BY on nullable column with allow_nullable_key disabled
      val exception = intercept[Exception] {
        writeData.write.format("clickhouse").mode(SaveMode.Append).options(options).save()
      }

      assert(
        exception.getMessage.contains("Sorting key") ||
          exception.getMessage.contains("nullable") ||
          exception.getMessage.contains("allow_nullable_key"),
        s"Expected error about nullable sorting key, got: ${exception.getMessage}"
      )
    } finally {
      dropTableWithRetry(dbName, tableName)
      if (!useSuiteLevelDatabase) {
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$dbName`")
      }
    }
  }
}
