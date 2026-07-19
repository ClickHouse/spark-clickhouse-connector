# ClickBench load-test dashboard — Apache Superset

Local [Apache Superset](https://superset.apache.org/) preview of the ClickBench
load-test metrics in the `perf.*` schema. Builds the same dashboard described by
`../queries.sql`: trend lines across runs, Spark-side metrics, throttling
indicators, and per-run drill-downs sliced by a native `Run` filter.

Reads the target ClickHouse connection from env vars — **no credentials are
stored in this repo**.

## Quick start

```bash
cd benchmarks/dashboard/superset

export TARGET_CH_HOST=<metrics clickhouse host>
export TARGET_CH_USER=<user>
export TARGET_CH_PASSWORD=<password>

docker compose up -d --build      # first run pulls/builds (~1-2 GB)
./bootstrap.sh                    # one-time: admin user + schema + roles

# create the database connection, datasets, charts, and dashboard:
docker cp ../queries.sql      clickbench-superset:/tmp/queries.sql
docker cp build_superset.py   clickbench-superset:/tmp/build_superset.py
docker cp describe_charts.py  clickbench-superset:/tmp/describe_charts.py
docker exec clickbench-superset python /tmp/build_superset.py
docker exec clickbench-superset python /tmp/describe_charts.py

open http://localhost:8088     # login admin / admin
```

Stop with `docker compose down` (add `-v` to also wipe the metadata volume).

## Files

| File | Purpose |
| --- | --- |
| `Dockerfile` | Superset image + the `clickhouse-connect` SQLAlchemy driver |
| `docker-compose.yml` | runs Superset locally; passes `TARGET_CH_*` through to the builder |
| `bootstrap.sh` | one-time Superset init (admin user, metadata schema, roles) |
| `build_superset.py` | idempotently creates the DB connection, virtual datasets, charts, dashboard, and the `Run` filter from `../queries.sql` |
| `describe_charts.py` | sets an explanatory description (with units) on each chart |

`build_superset.py` connects with
`clickhousedb://<user>:<password>@<host>:8443/perf?secure=true` (the
`clickhouse-connect` dialect — note `?secure=true`, not `?protocol=https`).

This is local-only preview tooling; it is not part of the load-test CI workflow.
