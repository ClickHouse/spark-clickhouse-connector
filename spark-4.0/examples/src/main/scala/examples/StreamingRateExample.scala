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
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Streaming example using rate source to write to ClickHouse.
 * This example generates synthetic data and writes it to ClickHouse in micro-batches.
 * 
 * Usage:
 * 1. Start ClickHouse (e.g., via Docker)
 * 2. Run this application
 * 3. Set breakpoints in the connector code to debug
 * 
 * The rate source generates rows with:
 * - timestamp: event time
 * - value: monotonically increasing long
 */
object StreamingRateExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClickHouse Streaming Rate Example")
      .master("local[*]")
      .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
      .config("spark.sql.catalog.clickhouse.host", "lia6gnmc4e.eu-west-1.aws.clickhouse-staging.com")
      .config("spark.sql.catalog.clickhouse.protocol", "https")
      .config("spark.sql.catalog.clickhouse.http_port", "8443")
      .config("spark.sql.catalog.clickhouse.user", "default")
      .config("spark.sql.catalog.clickhouse.password", "CvQow7_zxXxtU")
      .config("spark.sql.catalog.clickhouse.database", "default")
      .config("spark.sql.catalog.clickhouse.option.ssl", "true")
      // .config("spark.sql.catalog.clickhouse.option.http_keep_alive", "true")
      // .config("spark.sql.catalog.clickhouse.option.connect_timeout", "10s")
      // .config("spark.sql.catalog.clickhouse.option.socket_timeout", "30s")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 80)
    println("Starting ClickHouse Streaming Rate Example")
    println("=" * 80)

    // Create the target table in ClickHouse
    createClickHouseTable(spark)

    // Create a streaming DataFrame using rate source
    // Generates 10 rows per second
    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("rampUpTime", "0s")
      .option("numPartitions", "2")
      .load()

    // Transform the data to create a richer schema
    val enrichedStream = rateStream
      .withColumn("event_id", col("value"))
      .withColumn("event_time", col("timestamp"))
      .withColumn(
        "event_type",
        when(col("value") % 3 === 0, "type_a")
          .when(col("value") % 3 === 1, "type_b")
          .otherwise("type_c")
      )
      .withColumn("metric_value", (rand() * 100).cast("double"))
      .withColumn(
        "status",
        when(col("value") % 5 === 0, "completed")
          .when(col("value") % 5 === 1, "pending")
          .when(col("value") % 5 === 2, "failed")
          .when(col("value") % 5 === 3, "processing")
          .otherwise("queued")
      )
      .withColumn("user_id", (col("value") % 1000).cast("int"))
      .withColumn("session_id", concat(lit("session_"), (col("value") / 100).cast("int")))
      .withColumn(
        "metadata",
        to_json(struct(
          lit("source").as("source_system"),
          lit("v1.0").as("version"),
          col("value").as("sequence_number")
        ))
      )
      .select(
        "event_id",
        "event_time",
        "event_type",
        "metric_value",
        "status",
        "user_id",
        "session_id",
        "metadata"
      )

    // Write to ClickHouse using foreachBatch for better control and debugging
    val query = enrichedStream.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        println(s"\n${"=" * 80}")
        println(s"Processing Batch: $batchId")
        println(s"Batch Size: ${batchDF.count()} rows")
        println(s"${"=" * 80}")

        // Show sample data
        println("\nSample data from this batch:")
        batchDF.show(5, truncate = false)

        // Write to ClickHouse via catalog-aware V2 writer
        // This avoids relying on a DataSource short name (clickhouse.DefaultSource)
        batchDF.writeTo("clickhouse.default.streaming_events")
          .option("write_format", "arrow")
          .option("compression_codec", "none")
          .append()

        println(s"✓ Batch $batchId written successfully to ClickHouse\n")
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", "/tmp/clickhouse-streaming-checkpoint")
      .start()

    println("\n" + "=" * 80)
    println("Streaming query started. Press Ctrl+C to stop.")
    println("=" * 80)
    println("\nYou can query the data in ClickHouse:")
    println("  SELECT * FROM default.streaming_events ORDER BY event_time DESC LIMIT 10;")
    println("  SELECT event_type, count(*) FROM default.streaming_events GROUP BY event_type;")
    println("  SELECT status, avg(metric_value) FROM default.streaming_events GROUP BY status;")
    println("=" * 80 + "\n")

    // Wait for the query to terminate
    query.awaitTermination()
  }

  private def createClickHouseTable(spark: SparkSession): Unit = {
    println("\nCreating ClickHouse table if not exists...")

    try {
      spark.sql("""
        CREATE TABLE IF NOT EXISTS clickhouse.default.streaming_events (
          event_id BIGINT,
          event_time TIMESTAMP NOT NULL,
          event_type STRING,
          metric_value DOUBLE,
          status STRING,
          user_id INT,
          session_id STRING,
          metadata STRING
        )
        TBLPROPERTIES (
          engine = 'MergeTree()',
          order_by = 'event_time',
          partition_by = 'toYYYYMMDD(event_time)',
          settings.index_granularity = 8192
        )
      """)
      println("✓ Table created successfully or already exists\n")
    } catch {
      case e: Exception =>
        println(s"Warning: Could not create table: ${e.getMessage}")
        println("Make sure ClickHouse is running and accessible\n")
    }
  }
}
