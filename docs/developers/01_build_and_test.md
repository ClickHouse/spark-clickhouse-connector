---
license: |
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      https://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  
  See the License for the specific language governing permissions and
  limitations under the License.
---

Build and Test
===

## Build

Check out source code from GitHub

```
git checkout https://github.com/housepower/spark-clickhouse-connector.git
```

Build w/o test

```shell
./gradlew clean build -x test
```

Go to `spark-{{ spark_binary_version }}/clickhouse-spark-runtime/build/libs/` to find the output jar
`clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}-{{ version }}.jar`.

## Test

The project leverage [Testcontainers](https://www.testcontainers.org/) and [Docker Compose](https://docs.docker.com/compose/)
to do integration tests, you should install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
before running test, and check more details on [Testcontainers document](https://www.testcontainers.org/) if you'd
like to run test with remote Docker daemon.

Run all test

`./gradlew clean test`

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
