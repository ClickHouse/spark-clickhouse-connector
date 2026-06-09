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

# Agent Guidelines — Spark ClickHouse Connector

## What this is

- Spark DataSource V2 connector for ClickHouse.
- Talks to ClickHouse over HTTP via `com.clickhouse:client-v2`. **No JDBC at
  runtime (JDBC appears only as a test dependency), no V1 client.**
- Public surface (frozen names): `com.clickhouse.spark.ClickHouseCatalog`,
  `com.clickhouse.spark.ClickHouseTableProvider`, `ClickHouseTable`, the
  `clickhouse` short name, and the `com.clickhouse.spark` `groupId` / package.

## Supported versions

| | |
|--|--|
| **Spark** | 3.3, 3.4, 3.5, 4.0 |
| **Scala** | 2.12 (Spark 3.3 / 3.4 / 3.5) · 2.13 (all) — Spark 4.0 is 2.13 only |
| **Java** | 8 (Spark 3.3 / 3.4 / 3.5) · 17 (all) — Spark 4.0 requires 17 |

## Build & test

Pick exactly one Spark version per invocation. Use `-D` (system property), NOT
`-P`. Docker must be running for tests.

```bash
./gradlew clean build -x test -Dspark_binary_version=3.5 -Dscala_binary_version=2.13
./gradlew test          -Dspark_binary_version=3.5 -Dscala_binary_version=2.13
./gradlew spotlessApply -Dspark_binary_version=3.5
```

Run spotless **for every Spark version you touched**; CI fails otherwise.

## Jackson shading

**Version-specific.** Spark 4.0 bundles + relocates Jackson; 3.3 / 3.4 / 3.5
exclude it. Exclude transitive Jackson from any new dependency on all four
trees. (This is the one build constraint not covered by the references below.)

## Detailed references

The deeper guidance lives in `.claude/`. Read each of these files in full
before editing code — if your tooling has not already inlined them, open them
yourself:

- **Code layout, cross-version conventions, push-down surface, and the Java
  client (V2) contract:** @.claude/code-layout.md
- **Build/test failure modes, fat jar, single-suite runs, Cloud testing,
  Scala 2.12 cross-build pitfalls, and CI:** @.claude/build-and-test.md
- **Hard rules with rationale, the per-PR checklist, and the documentation
  policy:** @.claude/hard-rules.md
