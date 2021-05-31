Spark ClickHouse Connector
===

Build on Apache Spark DataSourceV2 API.

The project is currently on very early stage, and see unit tests to learn the usages and functionalities.

## Usage

The projects haven't published to Maven Central, you need to build and publish to private repository before using.

### Gradle

```
dependencies {
    implementation("com.github.xenon:spark-clickhouse-connector_2.12:$version")
}
```

### Maven
```
<dependency>
  <groupId>com.github.xenon</groupId>
  <artifactId>spark-clickhouse-connector_2.12</artifactId>
  <version>${version}</version>
</dependency>
```

### Version Format

`{year}.{month}.{date}[.{patch}][-SNAPSHOT]`

e.g.

- `2021.05.31-SNAPSHOT`
- `2021.05.31`
- `2021.05.31.1`

## Requirements

- Java 8
- Scala 2.12
- Apache Spark 3.1.x
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

The default version is `{year}.{month}.{date}[-SNAPSHOT]`, use `echo '1.0.0-stable' > version.txt` to custom version.

Publish Snapshots

`./gradlew publish`

Publish Release

`./gradlew -Prelease publish`
