Spark ClickHouse Connector
===
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.housepower/clickhouse-spark-runtime-3.2_2.12/badge.svg)](https://search.maven.org/search?q=clickhouse-spark-runtime)
[![License](https://img.shields.io/github/license/housepower/spark-clickhouse-connector)](https://github.com/housepower/spark-clickhouse-connector/blob/master/LICENSE)

Build on Apache Spark DataSourceV2 API.

## Usage

See the [documentation](https://housepower.github.io/spark-clickhouse-connector/) for how to use this connector.

## Requirements

- Java 8 or 11
- Scala 2.12 or 2.13
- Apache Spark 3.2 or 3.3 
- ClickHouse 21.1.2.15 or newer

Notes:
1. Currently, only support gRPC protocol, and ClickHouse support gRPC since
   [v21.1.2.15-stable](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md#clickhouse-release-v211215-stable-2021-01-18),
   but we only do test on v22.3.3.44-lts.

## Build

Build w/o test

`./gradlew clean build -x test`

## Test

The project leverage [Testcontainers](https://www.testcontainers.org/) and [Docker Compose](https://docs.docker.com/compose/)
to do integration tests, you should install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
before running test, and check more details on [Testcontainers document](https://www.testcontainers.org/) if you'd
like to run test with remote Docker daemon.

Run all test

`./gradlew clean test`

Run single test

`./gradlew test --tests=ConvertDistToLocalWriteSuite`
