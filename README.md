Spark ClickHouse Connector
===

Build on Apache Spark DataSourceV2 API.

## Usage

Goto [Home Page](https://housepower.github.io/spark-clickhouse-connector/) to learn how to use this connector.

## Requirements

- Java 8
- Scala 2.12
- Apache Spark 3.2.x
- ClickHouse 21.1.2.15 or newer

Notes:
1. Java 11 should work, but not tested.
2. Currently, only support gRPC protocol, and ClickHouse support gRPC since 
   [v21.1.2.15-stable](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md#clickhouse-release-v211215-stable-2021-01-18),
   but we only do test on v21.8.14.5-lts.

## Build

`./gradlew clean build -x test`

## Test

The project leverage [Testcontainers](https://www.testcontainers.org/) and Docker Compose to do integration tests, 
you should install Docker Desktop and Docker Compose before running test, and check more details on Testcontainers 
documents if you'd like to run test with remote Docker daemon.

`./gradlew clean test`

Run single test.

`./gradlew test --tests=ConvertDistToLocalWriteSuite`
