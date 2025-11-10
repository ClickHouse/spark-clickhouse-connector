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

package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Simple batch example for debugging the ClickHouse connector.
 * This example creates sample data and writes it to ClickHouse.
 * 
 * Usage:
 * 1. Start ClickHouse
 * 2. Set breakpoints in connector code
 * 3. Run this application in debug mode
 */
object SimpleBatchExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClickHouse Simple Batch Example")
      .master("local[*]")
      .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
      .config("spark.sql.catalog.clickhouse.host", "localhost")
      .config("spark.sql.catalog.clickhouse.protocol", "http")
      .config("spark.sql.catalog.clickhouse.http_port", "8123")
      .config("spark.sql.catalog.clickhouse.user", "default")
      .config("spark.sql.catalog.clickhouse.password", "")
      .config("spark.sql.catalog.clickhouse.database", "default")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 80)
    println("ClickHouse Simple Batch Example")
    println("=" * 80)

    // Create sample data
    import spark.implicits._
    val data = Seq(
      (1L, "Alice", 25, "Engineering", 75000.0, java.sql.Timestamp.valueOf("2024-01-15 10:30:00")),
      (2L, "Bob", 30, "Sales", 65000.0, java.sql.Timestamp.valueOf("2024-01-15 11:00:00")),
      (3L, "Charlie", 35, "Engineering", 85000.0, java.sql.Timestamp.valueOf("2024-01-15 11:30:00")),
      (4L, "Diana", 28, "Marketing", 70000.0, java.sql.Timestamp.valueOf("2024-01-15 12:00:00")),
      (5L, "Eve", 32, "Engineering", 90000.0, java.sql.Timestamp.valueOf("2024-01-15 12:30:00")),
      (6L, "Frank", 29, "Sales", 68000.0, java.sql.Timestamp.valueOf("2024-01-15 13:00:00")),
      (7L, "Grace", 31, "Marketing", 72000.0, java.sql.Timestamp.valueOf("2024-01-15 13:30:00")),
      (8L, "Henry", 27, "Engineering", 78000.0, java.sql.Timestamp.valueOf("2024-01-15 14:00:00")),
      (9L, "Ivy", 33, "Sales", 71000.0, java.sql.Timestamp.valueOf("2024-01-15 14:30:00")),
      (10L, "Jack", 26, "Engineering", 76000.0, java.sql.Timestamp.valueOf("2024-01-15 15:00:00"))
    ).toDF("employee_id", "name", "age", "department", "salary", "hire_date")

    println("\nSample data to write:")
    data.show(10, truncate = false)

    // Create table
    println("\nCreating table...")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS clickhouse.default.employees (
        employee_id BIGINT,
        name STRING,
        age INT,
        department STRING,
        salary DOUBLE,
        hire_date TIMESTAMP
      ) USING clickhouse
      TBLPROPERTIES (
        engine = 'MergeTree()',
        order_by = 'employee_id'
      )
    """)
    println("✓ Table created")

    // Write data - Set breakpoints in connector write code to debug
    println("\nWriting data to ClickHouse...")
    data.write
      .format("clickhouse")
      .mode("append")
      .option("write_format", "arrow")
      .save("clickhouse.default.employees")
    println("✓ Data written successfully")

    // Read data back - Set breakpoints in connector read code to debug
    println("\nReading data from ClickHouse...")
    val result = spark.read
      .format("clickhouse")
      .load("clickhouse.default.employees")
      .orderBy("employee_id")

    println("\nData read from ClickHouse:")
    result.show(10, truncate = false)

    // Perform aggregation
    println("\nAggregation by department:")
    result.groupBy("department")
      .agg(
        count("*").as("employee_count"),
        avg("salary").as("avg_salary"),
        avg("age").as("avg_age")
      )
      .orderBy(desc("avg_salary"))
      .show(truncate = false)

    println("\n" + "=" * 80)
    println("Example completed successfully!")
    println("=" * 80)

    spark.stop()
  }
}
