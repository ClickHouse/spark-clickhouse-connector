Spark ClickHouse Connector
===

Build on Apache Spark DataSourceV2 API.

The project is currently on very early stage, and see unit tests to learn the usages and functionalities.

## Usage

The projects haven't published to Maven Central, you need to build and publish to private repository before using.

### Gradle

```
dependencies {
    implementation("com.github.housepower:clickhouse-spark-32-runtime_2.12:$version")
}
```

### Maven
```
<dependency>
  <groupId>com.github.housepower</groupId>
  <artifactId>clickhouse-spark-32-runtime_2.12</artifactId>
  <version>${version}</version>
</dependency>
```

## Requirements

- Java 8
- Scala 2.12
- Apache Spark 3.2.x (not released yet)
- ClickHouse 21.3.x

Notes:
1. Java 11 should work, but not tested.
2. Currently, only support gRPC protocol, and ClickHouse support gRPC since 
   [v21.1.2.15-stable](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md#clickhouse-release-v211215-stable-2021-01-18),
   but we only do test on 21.3.x-lts.

## Build

`./gradlew clean build -x test`

## Test

The project leverage [Testcontainers](https://www.testcontainers.org/) and Docker Compose to do integration tests, 
you should install Docker Desktop and Docker Compose before running test, and check more details on Testcontainers 
documents if you'd like to run test with remote Docker daemon.

`./gradlew clean test`

## Publish to Private Repository

Config gradle in `~/.gradle/gradle.properties`

```
mavenUser=xxx
mavenPassword=xxx
mavenReleasesRepo=xxx
mavenSnapshotsRepo=xxx
```

Versions

Use content of `./version.txt` if the file exists, otherwise fallback to `{year}.{month}.{date}[-SNAPSHOT]`.

Publish Snapshots

`./gradlew publish`

Publish Release

`./gradlew -Prelease publish`
