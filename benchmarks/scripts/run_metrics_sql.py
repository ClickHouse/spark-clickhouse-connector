#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD,
              RUN_ID, RUN_START, TARGET_CH_HOST, TARGET_CH_USER,
              TARGET_CH_PASSWORD, CH_DATABASE, CH_TABLE
Optional env: RUN_END (defaults to RUN_START — unset for pre-ingest capture),
              SETTLE_END (defaults to RUN_END), SETTLE_SECONDS (default 0),
              SETTLE_TIMED_OUT (default 0), INPUT_PARQUET_GLOB (source glob,
              s3a:// or s3://; exposed to SQL as {source_glob} in s3:// form),
              DEFAULT_INPUT_PARQUET_GLOB, SOURCE_ROWS_EXPECTED,
              SOURCE_UNIQUE_EXPECTED (integrity source ground truth — see
              resolve_expected below),
              EVENT_LOG_URI, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
              AWS_SESSION_TOKEN
"""
import os
import sys

import ch_common


def resolve_expected(client, sql: str, source_glob: str):
    """Resolve the integrity check's source ground truth (contract §2.1).

    For the DEFAULT full glob the values MUST come from the constants pinned in
    the workflow env (SOURCE_ROWS_EXPECTED / SOURCE_UNIQUE_EXPECTED) — per-run
    re-derivation would uniqExact-scan the full ~100M-row source with GB-scale
    hash state on a memory-capped service, every run. Re-derivation via s3() is
    the fallback ONLY for non-default (smoke/override) globs, which are small.

    Lazy: only runs when the SQL actually references {rows_expected} /
    {unique_expected} (i.e. 20_insert_integrity.sql), so the other capture SQL
    files never trigger a source scan.
    """
    if "{rows_expected:" not in sql and "{unique_expected:" not in sql:
        return 0.0, 0.0
    default_glob = os.environ.get("DEFAULT_INPUT_PARQUET_GLOB", "")
    raw_glob = os.environ.get("INPUT_PARQUET_GLOB", "")
    const_rows = os.environ.get("SOURCE_ROWS_EXPECTED", "")
    const_uniq = os.environ.get("SOURCE_UNIQUE_EXPECTED", "")
    is_default = bool(raw_glob) and raw_glob == default_glob

    if is_default and const_rows and const_uniq:
        print(f"integrity: using pinned source constants (rows={const_rows}, "
              f"unique={const_uniq}) for the default glob", file=sys.stderr)
        return float(const_rows), float(const_uniq)

    if is_default:
        print("WARNING: default input glob but SOURCE_ROWS_EXPECTED / "
              "SOURCE_UNIQUE_EXPECTED are not both set — falling back to a "
              "FULL per-run source derivation (expensive full WatchID scan). "
              "Pin the constants in the workflow env.", file=sys.stderr)
    else:
        print(f"integrity: non-default glob — deriving source ground truth "
              f"from {source_glob}", file=sys.stderr)

    rows, uniq = client.query(
        "SELECT count(), uniqExact(WatchID) FROM s3({glob:String}, NOSIGN, 'Parquet')",
        parameters={"glob": source_glob},
    ).result_rows[0]
    print(f"integrity: derived source rows={rows}, unique={uniq}", file=sys.stderr)
    return float(rows), float(uniq)


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: run_metrics_sql.py <sql-file>")
    sql = open(sys.argv[1]).read()

    db = os.environ.get("CH_DATABASE", "")
    table = os.environ.get("CH_TABLE", "")
    # ClickHouse's s3() wants the s3:// scheme; the ingest glob is s3a:// (Spark).
    source_glob = os.environ.get("INPUT_PARQUET_GLOB", "")
    if source_glob.startswith("s3a://"):
        source_glob = "s3://" + source_glob[len("s3a://"):]

    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    rows_expected, unique_expected = resolve_expected(client, sql, source_glob)

    parameters = {
        "run_id": ch_common.require("RUN_ID"),
        "run_start": ch_common.require("RUN_START"),
        # RUN_END is unset for pre-ingest capture (e.g. pre-run covariates run
        # before the Spark step). It defaults to RUN_START then; those SQL files
        # do not reference {run_end} anyway.
        "run_end": os.environ.get("RUN_END") or os.environ.get("RUN_START", ""),
        "settle_end": os.environ.get("SETTLE_END") or os.environ.get("RUN_END")
        or os.environ.get("RUN_START", ""),
        "settle_seconds": float(os.environ.get("SETTLE_SECONDS", "0")),
        "settle_timed_out": float(os.environ.get("SETTLE_TIMED_OUT", "0")),
        "source_glob": source_glob,
        "rows_expected": rows_expected,
        "unique_expected": unique_expected,
        "event_log_uri": os.environ.get("EVENT_LOG_URI", ""),
        "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN", ""),
        # remoteSecure() reaches the target over native-secure 9440 regardless
        # of how we (the client) connect to the metrics service.
        "target_addr": f'{ch_common.require("TARGET_CH_HOST")}:9440',
        "target_user": os.environ.get("TARGET_CH_USER", ""),
        "target_password": os.environ.get("TARGET_CH_PASSWORD", ""),
        "ch_database": db,
        "ch_table": table,
        "table_qualified": f"{db}.{table}",
    }

    client.command(sql, parameters=parameters)


if __name__ == "__main__":
    main()
