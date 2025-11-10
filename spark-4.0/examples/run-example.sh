#!/bin/bash

# Script to run ClickHouse Spark examples with proper classpath
# Usage: ./run-example.sh [streaming|batch]

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/../.."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}ClickHouse Spark 4.0 Example Runner${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if ClickHouse is running
echo -e "\n${YELLOW}Checking ClickHouse connection...${NC}"
if curl -s http://localhost:8123/ping > /dev/null 2>&1; then
    echo -e "${GREEN}✓ ClickHouse is running${NC}"
else
    echo -e "${RED}✗ ClickHouse is not accessible at localhost:8123${NC}"
    echo -e "${YELLOW}Start ClickHouse with:${NC}"
    echo -e "  docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server"
    exit 1
fi

# Build the connector if needed
RUNTIME_JAR="$PROJECT_ROOT/spark-4.0/clickhouse-spark-runtime/build/libs/clickhouse-spark-runtime-4.0_2.13.jar"
if [ ! -f "$RUNTIME_JAR" ]; then
    echo -e "\n${YELLOW}Building connector...${NC}"
    cd "$PROJECT_ROOT"
    ./gradlew -Dspark_binary_version=4.0 -Dscala_binary_version=2.13 :clickhouse-spark-runtime-4.0_2.13:shadowJar --no-daemon
    echo -e "${GREEN}✓ Build complete${NC}"
else
    echo -e "${GREEN}✓ Connector runtime JAR found${NC}"
fi

# Determine which example to run
EXAMPLE_TYPE="${1:-streaming}"

case "$EXAMPLE_TYPE" in
    streaming|stream)
        EXAMPLE_CLASS="examples.StreamingRateExample"
        EXAMPLE_FILE="$SCRIPT_DIR/StreamingRateExample.scala"
        echo -e "\n${GREEN}Running Streaming Rate Example${NC}"
        ;;
    batch|simple)
        EXAMPLE_CLASS="examples.SimpleBatchExample"
        EXAMPLE_FILE="$SCRIPT_DIR/SimpleBatchExample.scala"
        echo -e "\n${GREEN}Running Simple Batch Example${NC}"
        ;;
    *)
        echo -e "${RED}Unknown example type: $EXAMPLE_TYPE${NC}"
        echo -e "Usage: $0 [streaming|batch]"
        exit 1
        ;;
esac

# Check if example file exists
if [ ! -f "$EXAMPLE_FILE" ]; then
    echo -e "${RED}✗ Example file not found: $EXAMPLE_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}Example class: $EXAMPLE_CLASS${NC}"
echo -e "${YELLOW}Runtime JAR: $RUNTIME_JAR${NC}"

# Find spark-submit
if command -v spark-submit &> /dev/null; then
    SPARK_SUBMIT="spark-submit"
elif [ -n "$SPARK_HOME" ] && [ -f "$SPARK_HOME/bin/spark-submit" ]; then
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
else
    echo -e "${RED}✗ spark-submit not found${NC}"
    echo -e "${YELLOW}Please install Spark 4.0 or set SPARK_HOME${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Using spark-submit: $SPARK_SUBMIT${NC}"

# Compile the example
echo -e "\n${YELLOW}Compiling example...${NC}"
CLASSES_DIR="$SCRIPT_DIR/target/classes"
mkdir -p "$CLASSES_DIR"

scalac -d "$CLASSES_DIR" \
    -classpath "$RUNTIME_JAR:$(find ~/.ivy2/cache/org.apache.spark -name 'spark-sql_2.13-4.0.*.jar' | head -1)" \
    "$EXAMPLE_FILE" 2>/dev/null || {
    echo -e "${YELLOW}Note: Compilation warnings can be ignored if using spark-submit${NC}"
}

# Run the example
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Starting Spark Application${NC}"
echo -e "${GREEN}========================================${NC}\n"

$SPARK_SUBMIT \
    --class "$EXAMPLE_CLASS" \
    --master "local[*]" \
    --driver-memory 2g \
    --conf "spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog" \
    --conf "spark.sql.catalog.clickhouse.host=localhost" \
    --conf "spark.sql.catalog.clickhouse.protocol=http" \
    --conf "spark.sql.catalog.clickhouse.http_port=8123" \
    --conf "spark.sql.catalog.clickhouse.user=default" \
    --conf "spark.sql.catalog.clickhouse.password=" \
    --conf "spark.sql.catalog.clickhouse.database=default" \
    --jars "$RUNTIME_JAR" \
    "$CLASSES_DIR"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Example completed${NC}"
echo -e "${GREEN}========================================${NC}"
