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

# Build, Test, and Style

AGENTS.md has the one-line build / test / spotless commands. This doc covers
the failure modes you cannot infer from those commands.

## Build

**Use `-D` (system property), not `-P` (Gradle project property).**
`settings.gradle` reads `System.getProperty("spark_binary_version")` /
`...("scala_binary_version")` directly (and throws
`Found unsupported Spark version: ...` on unknown values).
`gradle.properties` seeds those system properties via `systemProp.` entries
(currently `spark_binary_version=4.0`, `scala_binary_version=2.13`), so
`-Pspark_binary_version=3.5` is **silently ignored** — Gradle builds the
default (4.0 / 2.13) instead of 3.5, with no error. Always pass
`-Dspark_binary_version=...` and `-Dscala_binary_version=...` explicitly.

**Subproject names embed the Scala suffix:** `:clickhouse-spark-<v>_<scala>:`,
`:clickhouse-spark-it-<v>_<scala>:`, `:clickhouse-spark-runtime-<v>_<scala>:`
— not `:spark-<v>:clickhouse-spark:`. Run `./gradlew projects` to see the live
list. Direct task example:

```bash
./gradlew :clickhouse-spark-3.5_2.13:test --tests=ClickHouseGenericSuite \
  -Dspark_binary_version=3.5 -Dscala_binary_version=2.13
```

### Deployable fat jar

To produce just the shadow runtime jar (without running the full build):

```bash
./gradlew -Dspark_binary_version=3.5 -Dscala_binary_version=2.13 \
    :clickhouse-spark-runtime-3.5_2.13:shadowJar
```

Output:
`spark-3.5/clickhouse-spark-runtime/build/libs/clickhouse-spark-runtime-3.5_2.13-<version>.jar`.
`shadowJar` sets `archiveClassifier=null`, so there is **no `-all` suffix** —
the fat jar has the plain `…-<version>.jar` name and is easy to confuse with
the non-shadow `jar` output (whose classifier is `"empty"`). When iterating,
verify the jar's mtime before redeploying — stale-jar caching is silent.

---

## Tests

Tag-partitioned:

- Default `./gradlew test` excludes `org.scalatest.tags.Slow` and
  `org.scalatest.tags.Cloud`.
- `./gradlew slowTest` — includes Slow-tagged tests (e.g. `TPCDSClusterSuite`).
- `./gradlew cloudTest` — runs against a real ClickHouse Cloud instance.
  Requires `CLICKHOUSE_CLOUD_HOST` / `_USER` / `_PASSWORD` (optionally
  `_HTTP_PORT` defaulting to `8443`, `_TCP_PORT` defaulting to `9000`). See
  `ClickHouseCloudMixIn`.

`maxParallelForks=1` is intentional — IT suites share container state and
deadlock or cross-contaminate when run in parallel.

### ClickHouse Cloud SharedMergeTree is eventually consistent

Reads can race writes across replicas; `wait_for_async_insert=1` and TRUNCATE
acknowledgements do **not** guarantee immediate visibility on a different
replica. New Cloud tests must either:

1. Poll the SELECT until the expected row count appears, or
2. Add `spark.clickhouse.read.settings=select_sequential_consistency=1` to the
   test `SparkConf`. **`select_sequential_consistency=1` requires
   `insert_quorum_parallel=0`** on SharedMergeTree to take effect.

`Thread.sleep(...)` is not an acceptable primary strategy. Existing Cloud
tests in `SparkClickHouseSingleTest` and related fixtures still use bare
`Thread.sleep(2000)` guarded by `if (isCloud) ...` — that pattern is the
legacy approach this rule is intended to phase out, not a model to copy.

### Profile-switching pitfalls in one shell

The Gradle daemon caches `System.getProperty` values from the JVM it started
under, so back-to-back `for v in 3.3 3.4 3.5 4.0` loops can pick up the wrong
profile. Run each profile as a separate invocation with `--no-daemon`, or
call `./gradlew --stop` between profiles to evict the daemon. `--rerun-tasks`
alone does **not** address this.

Switching from Scala 2.13 to 2.12 (i.e. building 3.3 after 3.5) can also
leave stale 2.13 `.class` files in `clickhouse-core/build/` that the 2.12
compiler cannot read. On an unexplained Scala 2.12 compile failure in
`clickhouse-core` after a profile switch:

```bash
rm -rf clickhouse-core/build/classes clickhouse-core/build/libs
```

### Knobs

- `CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:<tag>` overrides the
  containerised CH image. `ClickHouseSingleMixIn` defaults to
  `clickhouse/clickhouse-server:25.3`, but the CI matrix exercises newer
  tags (see `.github/workflows/build-and-test.yml`). Local-green ≠ CI-green
  when a fix depends on behaviour that changed between minor versions.
- `DOCKER_API_VERSION` (default `1.44`) — passed to the test JVM as
  `-Dapi.version` (workaround for issue #503). Distinct from
  `DOCKER_MIN_API_VERSION` (Testcontainers env var, set only in CI).

### Test placement

- No Spark or CH dep → `clickhouse-core/src/test/`.
- Spark-agnostic CH integration tests → `clickhouse-core-it/src/test/`.
- Spark-version-specific suites → `spark-<v>/clickhouse-spark-it/src/test/`
  (further split into `single/` and `cluster/`).
- `ClickHouseSQLConf` golden-file tests → `spark-<v>/clickhouse-spark/src/test/scala/.../ConfigurationSuite.scala`
  (3.4 / 3.5 / 4.0 only; 3.3 predates it).

Spark-version-specific suites must be duplicated across `spark-3.3` …
`spark-4.0` the same way main code is.

---

## Code style

- `.scalafmt.conf` sets `runner.dialect=scala212` — this parses both 2.12 and
  2.13 sources. Do **not** change the dialect or the `scalaMajorVersion("2.12")`
  setting in `build.gradle` without testing the full matrix.
- **2.13-only language features compile cleanly on the 2.13 CI lane but
  break the 2.12 lane.** Watch for: string-interpolator extractors on the
  LHS of `case` (`case s"prefix-$x" =>`), the `LazyList` rename, certain
  collection methods (`.iterableFactory`, etc.). Neither Spotless nor the
  IDE will flag these; only the 2.12 CI matrix entries will.

---

## CI quick reference

Workflows in `.github/workflows/`:

- `build-and-test.yml` — full ClickHouse × Java × Scala × Spark matrix
  (Spark 4.0 excludes Scala 2.12 and Java 8). **Source of truth for the
  supported ClickHouse-version list.**
- `style.yml` — `spotlessCheck` per Spark version with matching Java
  (3.3/3.4/3.5 → Java 8, 4.0 → Java 17).
- `check-license.yml` — RAT license-header check.
- `cloud.yml` — `cloudTest` against real ClickHouse Cloud (uses secrets).
- `tpcds.yml` — TPC-DS (slow).

A green local build for one Spark version is not enough — multi-version
divergence is exactly what the matrix is designed to catch.
