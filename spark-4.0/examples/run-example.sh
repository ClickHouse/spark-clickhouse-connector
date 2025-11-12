#!/bin/bash

# Script to run ClickHouse Spark examples with proper classpath
# Usage: ./run-example.sh [streaming|batch]
# Optional env vars to override connection (must also reflect in your code or use Spark conf flags):
#   CH_HOST, CH_PROTOCOL (http|https), CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE

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

# Do not hard-fail on localhost availability; examples may target remote endpoints
if [ -z "$CH_HOST" ]; then CH_HOST="localhost"; fi
if [ -z "$CH_PROTOCOL" ]; then CH_PROTOCOL="http"; fi
if [ -z "$CH_PORT" ]; then CH_PORT="8123"; fi
if [ -z "$CH_USER" ]; then CH_USER="default"; fi
if [ -z "$CH_PASSWORD" ]; then CH_PASSWORD=""; fi
if [ -z "$CH_DATABASE" ]; then CH_DATABASE="default"; fi

echo -e "\n${YELLOW}Target ClickHouse: ${CH_PROTOCOL}://${CH_HOST}:${CH_PORT} (db=${CH_DATABASE})${NC}"

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

# Build the examples jar
EXAMPLES_JAR="$PROJECT_ROOT/spark-4.0/examples/build/libs/clickhouse-examples-4.0_2.13.jar"
echo -e "\n${YELLOW}Building examples jar...${NC}"
cd "$PROJECT_ROOT"
./gradlew -Dspark_binary_version=4.0 -Dscala_binary_version=2.13 :clickhouse-examples-4.0_2.13:jar --no-daemon
echo -e "${GREEN}✓ Examples jar built${NC}"

# Determine which example to run
EXAMPLE_TYPE="${1:-streaming}"

case "$EXAMPLE_TYPE" in
    streaming|stream)
        EXAMPLE_CLASS="examples.StreamingRateExample"
        echo -e "\n${GREEN}Running Streaming Rate Example${NC}"
        ;;
    batch|simple)
        EXAMPLE_CLASS="examples.SimpleBatchExample"
        echo -e "\n${GREEN}Running Simple Batch Example${NC}"
        ;;
    *)
        echo -e "${RED}Unknown example type: $EXAMPLE_TYPE${NC}"
        echo -e "Usage: $0 [streaming|batch]"
        exit 1
        ;;
esac

echo -e "${YELLOW}Example class: $EXAMPLE_CLASS${NC}"
echo -e "${YELLOW}Runtime JAR: $RUNTIME_JAR${NC}"
echo -e "${YELLOW}Examples JAR: $EXAMPLES_JAR${NC}"

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

# Run the example
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Starting Spark Application${NC}"
echo -e "${GREEN}========================================${NC}\n"

$SPARK_SUBMIT \
    --class "$EXAMPLE_CLASS" \
    --master "local[*]" \
    --driver-memory 2g \
    --driver-java-options "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" \
    --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" \
    --conf "spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog" \
    --conf "spark.sql.catalog.clickhouse.host=${CH_HOST}" \
    --conf "spark.sql.catalog.clickhouse.protocol=${CH_PROTOCOL}" \
    --conf "spark.sql.catalog.clickhouse.http_port=${CH_PORT}" \
    --conf "spark.sql.catalog.clickhouse.user=${CH_USER}" \
    --conf "spark.sql.catalog.clickhouse.password=${CH_PASSWORD}" \
    --conf "spark.sql.catalog.clickhouse.database=${CH_DATABASE}" \
    --jars "$RUNTIME_JAR" \
    "$EXAMPLES_JAR"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Example completed${NC}"
echo -e "${GREEN}========================================${NC}"
