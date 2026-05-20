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

# Code Layout & Cross-Version Conventions

## Module layout

Gradle multi-project build parameterized by `spark_binary_version` and
`scala_binary_version`. One invocation = one Spark version.

```
clickhouse-core/           Spark-agnostic core: NodeClient/ClusterClient,
                           NodeSpec/ClusterSpec/TableSpec, sharding hash
                           functions, format plumbing, SQL parser, test mixins.
clickhouse-core-it/        Core IT tests (no Spark dep).
spark-3.3/                 ┐
spark-3.4/                 ├─ Per-Spark-version trees. Each one contains:
spark-3.5/                 │    clickhouse-spark/          (main connector code)
spark-4.0/                 ┘    clickhouse-spark-runtime/  (shadowed runtime jar)
                                clickhouse-spark-it/       (Spark IT tests)
docker/                    Compose files for single-node + cluster CH.
.github/workflows/         CI.
```

## Where to edit

Per-Spark-version paths (identical across `spark-3.3/` … `spark-4.0/` unless
noted), under `clickhouse-spark/src/main/scala/`:

- `com/clickhouse/spark/`
  - `ClickHouseCatalog.scala` — `TableCatalog` + `SupportsNamespaces` +
    `FunctionCatalog`. Owns the long-lived `NodeClient`; discovers cluster
    topology on `initialize`.
  - `ClickHouseTableProvider.scala` — `TableProvider` for the `clickhouse`
    short name. Internally constructs a temporary `ClickHouseCatalog`; keep
    behavior consistent with the catalog path.
  - `ClickHouseTable.scala` — `Table` impl wired to read/write builders.
  - `read/` — `ClickHouseRead.scala` (Scan / ScanBuilder / push-downs),
    `ClickHouseReader.scala` (partition reader), `read/format/` readers.
  - `write/` — `ClickHouseWrite.scala` (WriteBuilder / BatchWrite),
    `ClickHouseWriter.scala` (DataWriter), `write/format/` writers.
  - `func/` — pushable scalar functions (xxHash64, MurmurHash2/3, CityHash64,
    MultiStringArgsHash) and the function registries.
- `org/apache/spark/sql/clickhouse/`
  - `ClickHouseSQLConf.scala` — Spark `SQLConf` entries (`spark.clickhouse.*`).
    Changes require a golden-file refresh (see AGENTS.md hard rule #5).
  - `ExprUtils.scala` — Spark `Expression` ↔ ClickHouse expression mapping.
    **Most cross-version drift lives here. Review with extreme care.**

Shared code lives in `clickhouse-core/src/main/scala/com/clickhouse/spark/`,
notably `client/` (`NodeClient`, `ClusterClient` — both `AutoCloseable`) and
`spec/` (`NodeSpec`, `ClusterSpec`, `TableSpec`, engines, partitioning).

---

## Cross-version duplication is intentional

The `spark-<v>/clickhouse-spark/` trees contain near-identical copies that
differ only where the underlying Spark API does. **Whenever you change a file
in one tree, you almost certainly need the same change in the others.** This
is the single most common defect in PRs to this repo.

### Push-down surface

The connector's read path implements a fixed set of Spark V2
`SupportsPushDown*` interfaces. Everything else (projections with
expressions, joins, sort without limit, window functions, etc.) runs in
Spark on the result rows. This is intentional, not an oversight.

| Concern | Hook in `ClickHouseRead.scala` | Translation contract |
|--|--|--|
| `WHERE` filters | `ClickHouseScanBuilder.pushFilters` (mixes `SupportsPushDownFilters`) | `SQLHelper.compileFilter(f): Option[String]`. Unsupported filters are returned to Spark for post-filtering — `null`/`None` is the supported "not pushable" signal. |
| `GROUP BY` + agg fns | `ClickHouseScanBuilder.pushAggregation` (mixes `SupportsPushDownAggregates`) | `SQLHelper.compileAggregate(AggregateFunc): Option[String]`. All-or-nothing per call. Limited to `MIN`/`MAX`/`COUNT`/`SUM`/`COUNT(*)` on a single-segment `NamedReference`. Issues a probe `... WHERE 1=0` to derive the output schema. |
| `LIMIT n` | `ClickHouseScanBuilder.pushLimit` (mixes `SupportsPushDownLimit`) | Stored as `_limit`, threaded into the final SQL. |
| Column pruning | `ClickHouseScanBuilder.pruneColumns` (mixes `SupportsPushDownRequiredColumns`) | Narrows `_readSchema` by name match. **No expression rewriting** — projections that are anything other than a column reference run in Spark. |
| Runtime/DPP filters | `ClickHouseBatchScan.{filter, filterAttributes}` (mixes `SupportsRuntimeFiltering`, gated by `spark.clickhouse.read.runtimeFilter.enabled`) | At `createReader` time, `runtimeFilters` are `compileFilters`-rendered and `AND`-merged into the per-partition `filtersExpr`. |
| Function lookup in expressions | `ExprUtils.toCatalyst` / `toSparkExpression` / `toSparkTransformOpt` + `FunctionRegistry` | V2 `ApplyTransform` whose name resolves in the `FunctionRegistry` is recognised; args walk recursively. |
| DDL `PARTITION BY` rendering | `ExprUtils.toClickHouse(transform: Transform): Expr` | Used at table create/alter time. Identifier quoting is the user's responsibility (DDL is user-typed). Do not "fix" this in lockstep with read-path quoting. |
| Catalog functions (`SHOW FUNCTIONS`, etc.) | `ClickHouseCatalog.{listFunctions, loadFunction}` (`extends FunctionCatalog`) | Backed by `StaticFunctionRegistry` (built-in hash funcs) + a `DynamicFunctionRegistry` populated at `initialize` (per-cluster shard hash funcs, e.g. `clickhouse_shard_xxHash64`). |

**Explicit non-goals (and how Spark falls back):**

- **No projection push-down.** Spark V2 has no `SupportsPushDownProjection`
  at all — projection is column pruning (`SupportsPushDownRequiredColumns`)
  only. `ClickHouseScanBuilder.pruneColumns` matches by **top-level field
  name** (`requiredCols.contains(field.name)`), so nested struct/map/array
  members are still fetched in full and any expression beyond a bare column
  reference (`col + 1`, `length(s)`, `s.a`) runs in Spark.
- **No join push-down.** `SupportsPushDownJoin` (SPARK-52187) only exists
  in Spark 4.1+, which the connector does not target yet. Zero matches for
  `JoinPushdown|pushDownJoin|SupportsPushDownJoin` on `main`. Joins run
  as two independent `ClickHouseBatchScan`s plus a Spark-side join.
  `ClickHouseCommandRunner` is **not** a join escape hatch — it's an
  `ExternalCommandRunner` returning `Array[String]`, intended for DDL.
- **No source-side expression evaluation beyond `SQLHelper`.** Bugs in
  `compileFilter` / `compileAggregate` silently produce wrong results, not
  exceptions. Add tests that assert the generated SQL string, not just the
  result rows.

### Cross-version drift

The four trees diverge in narrow, predictable places. Diff carefully:

- **Spark 4.0 reworked `AnalysisException` for error classes.** 4.0:
  `new AnalysisException(errorClass = "...", messageParameters = Map(...))`.
  3.3 / 3.4 / 3.5 still use the positional
  `new AnalysisException(message, cause = Some(e))`. The
  `UnsupportedOperationException` handler in `ExprUtils.scala` is the
  canonical place this shows up; compare each tree before copying.
- **`ExprUtils` function-translation signatures differ between 3.3 and
  3.4+.** 3.4 / 3.5 / 4.0 thread a `functionRegistry: FunctionRegistry`
  argument through `toCatalyst`, `toSparkTransformOpt`, `toSparkExpression`,
  and `toClickHouse`. 3.3 has no `FunctionRegistry` parameter on these
  signatures — `ApplyTransform` resolution is narrower as a result, and any
  push-down that wants to translate a Spark function call into a ClickHouse
  one must implement the 3.3 path more conservatively. Do not paste a 3.4+
  patch into 3.3 without auditing every call site.
- **`SupportsPushDown*` mix-in list on `ClickHouseScanBuilder` is uniform
  on `main` today** (Limit + Filters + Aggregates + RequiredColumns) but
  is *not* guaranteed to stay uniform across versions if a future Spark
  release adds an interface only 4.0 implements. Always grep the actual
  `extends ScanBuilder with ...` line in each tree before assuming
  identical surface.

### `VariantType` is Spark 4.0-only

The symbols `VariantType`, `VariantVal`, and `Variant` do not exist on the
Spark 3.x classpath. Variant-handling code — the 4.0 `ClickHouseArrowStreamWriter`
override of `revisedDataSchema`, its `variantToJsonString` helper,
schema-mapping branches, related tests — lives **only** in `spark-4.0/`.
(The base `ClickHouseWriter.revisedDataSchema` field itself exists in every
version; only the Variant-rewrite override is 4.0-only.)

---

## The Java client (V2) — non-negotiables

All real I/O goes through `com.clickhouse.spark.client.NodeClient`, which
wraps `com.clickhouse.client.api.Client` from `com.clickhouse:client-v2`.

- **Never use the deprecated V1 API** — the symbols to grep against are
  `com.clickhouse.client.ClickHouseClient`, `ClickHouseRequest`,
  `ClickHouseResponse`. V2 only (`com.clickhouse.client.api.*`).
- `Client` and `NodeClient` are `AutoCloseable`. The catalog owns the
  top-level instance; per-shard clients live in `ClusterClient`.
- **Read wire formats:** `json` → `JSONCompactEachRowWithNamesAndTypes`,
  `binary` → `RowBinaryWithNamesAndTypes`. Selected via
  `spark.clickhouse.read.format`.
- **Write wire formats:** `json` → `JSONEachRow`, `arrow` → `ArrowStream`.
  Selected via `spark.clickhouse.write.format`.
- **Read and write format names are not symmetric** — `arrow` is write-only,
  `binary` is read-only. New data types must be wired through every format
  the user could pick on either side, not just the default.
- **`Client.Builder.setOption(...)` silently drops arbitrary ClickHouse
  server settings.** Only keys prefixed with `clickhouse_setting_` are
  translated into URL params / `SETTINGS` clauses; everything else is stored
  on the client config but never reaches the server. Use catalog
  `option.clickhouse_setting_<name>=<value>`, or
  `spark.clickhouse.read.settings=<k>=<v>,...` (literal `SETTINGS` clause on
  the SELECT). **There is no `spark.clickhouse.write.settings`** — for
  write-path server settings, use `option.clickhouse_setting_*`.
- **Arrow write, Variant handling (Spark 4.0 only):**
  `ClickHouseWriter.revisedDataSchema` exists in every version as the schema
  the formats serialize against (a few generic Spark→CH-friendly rewrites).
  In **`spark-4.0/clickhouse-spark/.../write/format/ClickHouseArrowStreamWriter.scala`**
  it is **overridden** to additionally rewrite every `VariantType` field to
  `StringType`, and the writer calls a local `variantToJsonString` helper to
  serialize each value to JSON before handing it to the Arrow writer. This
  is an intentional workaround, not dead code: ClickHouse's Arrow input
  format does not recognise Arrow `Union`, the `arrow.variant` extension
  type, or any other structural Variant encoding. Do not "clean it up"
  without first verifying ClickHouse's Arrow reader can consume the new
  encoding end-to-end.
