# Spark 4.0 ClickHouse Connector Examples

This directory contains example applications for debugging and testing the ClickHouse connector with Spark 4.0.

## Prerequisites

1. **ClickHouse Server Running**
   ```bash
   # Using Docker
   docker run -d --name clickhouse-server \
     -p 8123:8123 -p 9000:9000 \
     --ulimit nofile=262144:262144 \
     clickhouse/clickhouse-server
   ```

2. **Build the Connector** (optional - only needed for local debugging)
   
   By default, examples use the connector from Maven. To debug connector code changes:
   
   ```bash
   # From project root - build the local connector JAR
   ./gradlew :clickhouse-spark-runtime-4.0_2.13:shadowJar -Dspark_binary_version=4.0 -Dscala_binary_version=2.13
   ```
   
   Then uncomment the `unmanagedJars` section in `build.sbt` to use the local JAR.

## Running Examples in IDE (for Debugging)

### IntelliJ IDEA / VS Code with Metals

1. **Import the project** as a Gradle project

2. **Set up Run Configuration**:
   - Main class: `examples.StreamingRateExample` or `examples.SimpleBatchExample`
   - VM options:
     ```
     -Dspark_binary_version=4.0
     -Dscala_binary_version=2.13
     ```
   - Working directory: `spark-4.0/examples`
   - Classpath: Include `clickhouse-spark-4.0_2.13` module

3. **Set Breakpoints** in connector code:
   - Write path: `com.clickhouse.spark.write.ClickHouseWriter`
   - Read path: `com.clickhouse.spark.read.ClickHouseReader`
   - Catalog operations: `com.clickhouse.spark.ClickHouseCatalog`

4. **Run in Debug Mode** and step through the connector code

## Running Examples

### Option 1: Using Gradle (Recommended)

**SimpleBatchExample:**
```bash
# From project root
CH_PORT=8123 ./gradlew :examples-scala-spark-4.0:runBatch \
  -Dspark_binary_version=4.0 -Dscala_binary_version=2.13
```

**StreamingRateExample:**
```bash
# From project root
CH_PORT=8123 ./gradlew :examples-scala-spark-4.0:runStreaming \
  -Dspark_binary_version=4.0 -Dscala_binary_version=2.13
```

**Environment Variables:**
- `CH_HOST` - ClickHouse host (default: localhost)
- `CH_PORT` - ClickHouse HTTP port (default: 8123)
- `CH_PROTOCOL` - http or https (default: http)
- `CH_USER` - ClickHouse user (default: default)
- `CH_PASSWORD` - ClickHouse password (default: empty)
- `CH_DATABASE` - ClickHouse database (default: default)

### Option 2: Using sbt

**Note:** sbt will automatically download the connector from Maven Central (version `0.9.0`).

**SimpleBatchExample:**
```bash
# From examples/scala/spark-4.0 directory
CH_PORT=8123 sbt "runMain examples.SimpleBatchExample"
```

**StreamingRateExample:**
```bash
# From examples/scala/spark-4.0 directory
CH_PORT=8123 sbt "runMain examples.StreamingRateExample"
```

**For local debugging:** Uncomment the `unmanagedJars` section in `build.sbt` to use a locally built connector JAR instead of Maven.

### Option 3: Using spark-submit

**SimpleBatchExample:**
```bash
spark-submit \
  --class examples.SimpleBatchExample \
  --master local[*] \
  --jars /path/to/clickhouse-spark-runtime-4.0_2.13-0.9.0-SNAPSHOT.jar \
  /path/to/examples-scala-spark-4.0.jar
```

**StreamingRateExample:**
```bash
spark-submit \
  --class examples.StreamingRateExample \
  --master local[*] \
  --jars /path/to/clickhouse-spark-runtime-4.0_2.13-0.9.0-SNAPSHOT.jar \
  /path/to/examples-scala-spark-4.0.jar
```

## Examples Description

### 1. SimpleBatchExample

A straightforward batch processing example that:
- Creates sample employee data
- Writes to ClickHouse
- Reads back and performs aggregations

**Good for debugging**:
- Table creation logic
- Batch write operations
- Read operations
- Schema inference

### 2. StreamingRateExample

A streaming application that:
- Uses Spark's rate source (generates synthetic data)
- Enriches data with multiple columns
- Writes to ClickHouse in micro-batches every 5 seconds
- Generates 10 rows per second

**Good for debugging**:
- Streaming write operations
- Micro-batch processing
- Continuous data ingestion
- Performance under load

**Monitor the stream**:
```sql
-- In ClickHouse client
SELECT count(*) FROM default.streaming_events;

SELECT 
  event_type, 
  count(*) as cnt, 
  avg(metric_value) as avg_metric
FROM default.streaming_events 
GROUP BY event_type;

SELECT 
  toStartOfMinute(event_time) as minute,
  count(*) as events_per_minute
FROM default.streaming_events
GROUP BY minute
ORDER BY minute DESC
LIMIT 10;
```

## Debugging Tips

### Enable Debug Logging

Add to your SparkSession configuration:
```scala
.config("spark.sql.catalog.clickhouse.option.log.level", "DEBUG")
```

Or set log level programmatically:
```scala
spark.sparkContext.setLogLevel("DEBUG")
```

### Useful ClickHouse Queries

```sql
-- Check table structure
DESCRIBE TABLE default.streaming_events;

-- Check table engine and settings
SHOW CREATE TABLE default.streaming_events;

-- Monitor inserts
SELECT 
  table,
  sum(rows) as total_rows,
  sum(bytes) as total_bytes
FROM system.parts
WHERE database = 'default' AND table IN ('streaming_events', 'employees')
GROUP BY table;

-- Check recent parts
SELECT 
  partition,
  name,
  rows,
  bytes_on_disk,
  modification_time
FROM system.parts
WHERE database = 'default' AND table = 'streaming_events'
ORDER BY modification_time DESC
LIMIT 10;
```

## Troubleshooting

### Connection Issues

If you see connection errors:
```scala
// Verify ClickHouse is accessible
curl http://localhost:8123/ping
```

### Clean Up

```sql
-- Drop tables
DROP TABLE IF EXISTS default.streaming_events;
DROP TABLE IF EXISTS default.employees;
```

```bash
# Remove checkpoint directory
rm -rf /tmp/clickhouse-streaming-checkpoint
```
