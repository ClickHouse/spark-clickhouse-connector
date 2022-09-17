Spark ClickHouse Connector
===
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.housepower/clickhouse-spark-runtime-3.3_2.12/badge.svg)](https://search.maven.org/search?q=clickhouse-spark-runtime)
[![License](https://img.shields.io/github/license/housepower/spark-clickhouse-connector)](https://github.com/housepower/spark-clickhouse-connector/blob/master/LICENSE)

Build on Apache Spark DataSourceV2 API.

![Overview](docs/imgs/scc_overview.drawio.png)

## Usage

See the [documentation](https://housepower.github.io/spark-clickhouse-connector/) for how to use this connector.

## Requirements

- Java 8 or 11
- Scala 2.12 or 2.13
- Apache Spark 3.3

Notes:
1. As of 0.5.0, this connector switches from ClickHouse raw gRPC Client to 
   [ClickHouse Official Java Client](https://github.com/ClickHouse/clickhouse-jdbc), which brings HTTP protocol support,
   extending the range of supported versions of ClickHouse Server.

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

Run all test w/ Spark 3.3 and Scala 2.13

`./gradlew clean test -Dspark_binary_version=3.3 -Dscala_binary_version=2.13`

Run single test

`./gradlew test --tests=ConvertDistToLocalWriteSuite`

### ARM Platform

For developers/users who use ARM platform, e.g. [Apple Silicon](https://developer.apple.com/documentation/apple-silicon)
chips, [Kunpeng](https://www.hikunpeng.com/) chips, you may not able to run TPC-DS integrations test using gRPC in local directly,
because [ClickHouse does not provide gRPC support in official ARM image](https://github.com/ClickHouse/ClickHouse/pull/36754).

As a workaround, you can set the environment variable `CLICKHOUSE_IMAGE` to use a custom image which supports gRPC
on ARM platform for testing.

```
export CLICKHOUSE_IMAGE=pan3793/clickhouse-server:22.5.1-alpine-arm-grpc
./gradlew clean test
```
