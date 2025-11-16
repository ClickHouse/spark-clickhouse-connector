<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Debugging Guide for ClickHouse Spark Connector

This guide shows how to debug the connector while running the example applications.

## Setup for IntelliJ IDEA

### 1. Import Project

1. Open IntelliJ IDEA
2. File → Open → Select the project root directory
3. Import as Gradle project
4. Wait for indexing to complete

### 2. Build the Connector

Before debugging, build the connector modules:

```bash
./gradlew -Dspark_binary_version=4.0 -Dscala_binary_version=2.13 \
  :clickhouse-core:build \
  :clickhouse-spark-4.0_2.13:build
```

### 3. Create Run Configuration

#### For StreamingRateExample:

1. Run → Edit Configurations → Add New → Application
2. Configure:
   - **Name**: `StreamingRateExample (Debug)`
   - **Main class**: `examples.StreamingRateExample`
   - **VM options**:
     ```
     -Dspark_binary_version=4.0
     -Dscala_binary_version=2.13
     -Dspark.master=local[*]
     ```
   - **Working directory**: `$PROJECT_DIR$/spark-4.0/examples`
   - **Use classpath of module**: `spark-clickhouse-connector.spark-4.0.clickhouse-spark-4.0_2.13.main`
   - **JRE**: Java 17 (temurin-17)

3. Click Apply and OK

#### For SimpleBatchExample:

Same as above but change:
- **Name**: `SimpleBatchExample (Debug)`
- **Main class**: `examples.SimpleBatchExample`

### 4. Set Breakpoints

Open the connector source files and set breakpoints at key locations:

#### Write Path Breakpoints:

1. **`ClickHouseWriter.scala`** (line ~180)
   ```scala
   override def write(record: InternalRow): Unit = {
     // Breakpoint here to see each row being written
   ```

2. **`ClickHouseArrowStreamWriter.scala`** (line ~50)
   ```scala
   override def writeRow(record: InternalRow): Unit = {
     // Breakpoint here to debug Arrow serialization
   ```

3. **`ClickHouseWrite.scala`** (line ~80)
   ```scala
   override def commit(messages: Array[WriterCommitMessage]): Unit = {
     // Breakpoint here to see commit phase
   ```

#### Read Path Breakpoints:

1. **`ClickHouseReader.scala`** (line ~70)
   ```scala
   override def next(): Boolean = {
     // Breakpoint here to debug record reading
   ```

2. **`ClickHouseBinaryReader.scala`**
   ```scala
   override def get(): InternalRow = {
     // Breakpoint here to see row deserialization
   ```

#### Catalog Breakpoints:

1. **`ClickHouseCatalog.scala`** (line ~290)
   ```scala
   override def createTable(...): Table = {
     // Breakpoint here to debug table creation
   ```

2. **`ClickHouseHelper.scala`** (line ~150)
   ```scala
   def queryTableSpec(...): TableSpec = {
     // Breakpoint here to see table metadata queries
   ```

### 5. Start Debugging

1. Make sure ClickHouse is running:
   ```bash
   docker run -d --name clickhouse-server \
     -p 8123:8123 -p 9000:9000 \
     clickhouse/clickhouse-server
   ```

2. Click the Debug icon (🐛) next to your run configuration
3. The application will start and pause at your breakpoints

## Debugging Tips

### Inspect Variables

When stopped at a breakpoint, you can inspect:

- **`record: InternalRow`** - The Spark internal row being processed
- **`schema: StructType`** - The DataFrame schema
- **`options: CaseInsensitiveStringMap`** - Configuration options
- **`nodeClient: NodeClient`** - ClickHouse connection client

### Evaluate Expressions

Use the "Evaluate Expression" window (Alt+F8) to:

```scala
// Check row values
record.get(0, StringType)
record.getInt(1)

// Inspect schema
schema.fields.map(_.name).mkString(", ")

// Check options
options.get("write_format")
```

### Step Through Code

- **F8** - Step over
- **F7** - Step into
- **Shift+F8** - Step out
- **F9** - Resume program

### Watch Expressions

Add watches for:
- `currentBufferedRows` - Number of rows in current buffer
- `totalRecordsWritten` - Total records written
- `totalSerializeTime` - Time spent serializing

### Conditional Breakpoints

Right-click a breakpoint to add conditions:

```scala
// Only break on specific row values
record.getLong(0) == 100

// Only break after N iterations
currentBufferedRows > 1000

// Only break on errors
exception != null
```

## VS Code with Metals

### 1. Install Extensions

- Scala (Metals)
- Debugger for Java

### 2. Create launch.json

`.vscode/launch.json`:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "scala",
      "request": "launch",
      "name": "StreamingRateExample",
      "mainClass": "examples.StreamingRateExample",
      "args": [],
      "jvmOptions": [
        "-Dspark_binary_version=4.0",
        "-Dscala_binary_version=2.13"
      ],
      "buildTarget": "clickhouse-spark-4.0_2.13"
    }
  ]
}
```

### 3. Import Build

1. Open Command Palette (Cmd+Shift+P)
2. Run "Metals: Import build"
3. Wait for import to complete

### 4. Start Debugging

1. Set breakpoints by clicking in the gutter
2. Press F5 or click Run → Start Debugging
3. Select "StreamingRateExample" configuration

## Common Debugging Scenarios

### Scenario 1: Debug Write Performance

**Goal**: Understand why writes are slow

**Breakpoints**:
1. `ClickHouseWriter.flush()` - See when flushes occur
2. `ClickHouseArrowStreamWriter.doSerialize()` - Measure serialization time
3. `NodeClient.syncQueryAndCheck()` - Measure network time

**What to check**:
- `currentBufferedRows` - Buffer size
- `totalSerializeTime` - Serialization overhead
- `totalWriteTime` - Network overhead

### Scenario 2: Debug Schema Mapping

**Goal**: Verify Spark types map correctly to ClickHouse types

**Breakpoints**:
1. `SchemaUtils.toClickHouseSchema()` - Spark → ClickHouse conversion
2. `SchemaUtils.fromClickHouseSchema()` - ClickHouse → Spark conversion

**What to check**:
- Input `StructType` fields
- Output ClickHouse column types
- Nullable handling

### Scenario 3: Debug Streaming Micro-batches

**Goal**: See how each micro-batch is processed

**Breakpoints**:
1. In `foreachBatch` lambda in `StreamingRateExample`
2. `ClickHouseWrite.createBatchWriterFactory()`
3. `ClickHouseWriter.commit()`

**What to check**:
- `batchId` - Current batch number
- `batchDF.count()` - Batch size
- Commit messages

### Scenario 4: Debug Connection Issues

**Goal**: Troubleshoot ClickHouse connection problems

**Breakpoints**:
1. `NodeClient.syncQueryAndCheck()` - HTTP requests
2. `ClickHouseHelper.buildNodeSpec()` - Connection configuration

**What to check**:
- `nodeSpec.host` and `nodeSpec.port`
- Request URL and parameters
- Response status codes

## Logging

### Enable Debug Logging

Add to your example code:

```scala
import org.apache.log4j.{Level, Logger}

// Set connector logging to DEBUG
Logger.getLogger("com.clickhouse.spark").setLevel(Level.DEBUG)

// Set Spark SQL logging to INFO
Logger.getLogger("org.apache.spark.sql").setLevel(Level.INFO)
```

### View Logs

Logs will appear in:
- IntelliJ: Run window (bottom panel)
- VS Code: Debug Console
- Terminal: stdout/stderr

## Performance Profiling

### Using IntelliJ Profiler

1. Run → Profile 'StreamingRateExample'
2. Let it run for 30-60 seconds
3. Stop the profiler
4. Analyze:
   - CPU flame graph
   - Memory allocation
   - Method call tree

### Key Methods to Profile

- `ClickHouseWriter.write()` - Per-row overhead
- `doSerialize()` - Serialization cost
- `NodeClient.syncQueryAndCheck()` - Network I/O

## Troubleshooting

### Breakpoints Not Hit

- Verify the module is included in classpath
- Check that code is compiled (Build → Rebuild Project)
- Ensure you're debugging, not just running

### Cannot Resolve Symbols

- File → Invalidate Caches → Invalidate and Restart
- Reimport Gradle project
- Check Scala SDK is configured

### ClassNotFoundException

- Build the connector modules first
- Check run configuration classpath includes connector modules
- Verify Spark dependencies are available

## Next Steps

Once comfortable with debugging:

1. Try modifying connector code and see changes in real-time
2. Add custom metrics or logging
3. Experiment with different write formats (Arrow vs JSON)
4. Test error handling by simulating failures
5. Profile and optimize hot paths
