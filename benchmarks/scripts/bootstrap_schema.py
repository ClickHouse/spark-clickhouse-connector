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
Required env: TARGET_CH_HOST, TARGET_CH_USER, TARGET_CH_PASSWORD,
              METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD
"""
import os
import re

import ch_common

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SQL = os.path.join(REPO_ROOT, "benchmarks", "sql")

TARGET_DDL = [
    os.path.join(SQL, "clickbench", "01_create_database.sql"),
    os.path.join(SQL, "clickbench", "02_create_hits.sql"),
    # Tier-0 target: ENGINE=Null table on the SAME Cloud target as hits
    # (redesign 2026-07-07 — drops the Docker-on-master instrument). Bootstrapped
    # here alongside hits, idempotent CREATE IF NOT EXISTS. See contract §1.1
    # (Cloud-hosted Null branch) and benchmarks/tier0/README.md.
    os.path.join(SQL, "clickbench", "03_create_hits_null.sql"),
]
METRICS_DDL = [
    os.path.join(SQL, "perf", "01_create_database.sql"),
    os.path.join(SQL, "perf", "02_create_runs.sql"),
    os.path.join(SQL, "perf", "03_create_metrics.sql"),
    os.path.join(SQL, "perf", "04_create_ch_inserts.sql"),
]


# CREATE TABLE [IF NOT EXISTS] [db.]table  — capture optional db + table name.
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:`?(?P<db>\w+)`?\.)?`?(?P<table>\w+)`?",
    re.IGNORECASE,
)
# PARTITION BY <expr>  — expr runs up to the next clause keyword or statement end.
_PARTITION_BY_RE = re.compile(
    r"PARTITION\s+BY\s+(?P<expr>.+?)"
    r"(?=\s+(?:ORDER\s+BY|PRIMARY\s+KEY|SAMPLE\s+BY|TTL|SETTINGS)\b|\s*;|\s*$)",
    re.IGNORECASE | re.DOTALL,
)


def _norm(expr: str) -> str:
    """Normalize a partition expression for comparison: strip whitespace so
    'toYYYYMM(EventDate)' matches regardless of incidental spacing."""
    return re.sub(r"\s+", "", expr or "").strip()


def _strip_sql_comments(sql: str) -> str:
    # Drop -- line comments so the regexes don't match inside the license header.
    return re.sub(r"--[^\n]*", "", sql)


def parse_ddl_intent(sql: str):
    """Return (db, table, intended_partition_key) parsed from a CREATE TABLE DDL,
    or None if the file is not a CREATE TABLE (e.g. CREATE DATABASE).

    intended_partition_key is the normalized PARTITION BY expression, or '' when
    the DDL has no PARTITION BY clause (e.g. the ENGINE=Null hits_null table)."""
    body = _strip_sql_comments(sql)
    m = _CREATE_TABLE_RE.search(body)
    if not m:
        return None
    db = m.group("db")
    table = m.group("table")
    pm = _PARTITION_BY_RE.search(body)
    intended = _norm(pm.group("expr")) if pm else ""
    return db, table, intended


def reconcile_partition_key(client, sql):
    """Guard against the CREATE-IF-NOT-EXISTS migration trap.

    bootstrap applies every DDL with CREATE TABLE IF NOT EXISTS, so an already
    existing table is NEVER recreated — editing a PARTITION BY in the DDL would
    silently no-op on the long-lived Cloud target. This detects that case
    generically: it compares the DDL's intended PARTITION BY expression against
    the live table's system.tables.partition_key and, on mismatch, DROPs the
    table so the subsequent CREATE recreates it with the new scheme.

    Data loss on DROP is intentional and safe: the benchmark TRUNCATEs the table
    every run, so it holds no durable data.

    No-ops (never DROPs) when:
      * the DDL is not a CREATE TABLE (e.g. CREATE DATABASE),
      * the DDL declares no PARTITION BY (e.g. clickbench.hits_null, ENGINE=Null)
        — an unpartitioned intent never triggers a recreate,
      * the table does not yet exist (first bootstrap — CREATE handles it),
      * the live partition_key already matches the intended one (idempotent).
    """
    intent = parse_ddl_intent(sql)
    if intent is None:
        return  # not a CREATE TABLE (CREATE DATABASE etc.)
    db, table, intended = intent
    if not intended:
        # No partitioning intended (e.g. ENGINE=Null hits_null) — nothing to
        # reconcile; an empty live partition_key is the correct state.
        return
    if not db:
        # DDL used an unqualified table name; we cannot scope the introspection
        # safely (would need the connection's current database). All bootstrap
        # DDLs qualify the db, so this should not happen — skip loudly.
        print(f"    [migrate] {table}: DDL has no explicit database; skipping "
              f"partition-key reconcile")
        return

    rows = client.query(
        "SELECT partition_key FROM system.tables "
        "WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": db, "tbl": table},
    ).result_rows
    if not rows:
        print(f"    [migrate] {db}.{table}: does not exist yet — CREATE will "
              f"make it with PARTITION BY {intended}")
        return
    actual = _norm(rows[0][0])
    if actual == intended:
        print(f"    [migrate] {db}.{table}: partition key OK "
              f"(PARTITION BY {intended}) — no recreate")
        return

    print(f"    [migrate] {db}.{table}: PARTITION-KEY MISMATCH "
          f"live='{actual}' intended='{intended}' — DROPPING and recreating. "
          f"Data loss is safe: the table is TRUNCATEd every run.")
    client.command(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    print(f"    [migrate] {db}.{table}: dropped; CREATE below will recreate it "
          f"with PARTITION BY {intended}")


def apply_all(client, ddl_files):
    for path in ddl_files:
        rel = path.split("/sql/", 1)[-1]
        print(f"  applying {rel}")
        sql = open(path).read()
        reconcile_partition_key(client, sql)
        client.command(sql)


def main() -> None:
    target = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")
    print(f"[bootstrap] target service {os.environ['TARGET_CH_HOST']}")
    apply_all(target, TARGET_DDL)

    if os.environ.get("METRICS_CH_HOST") != os.environ.get("TARGET_CH_HOST"):
        print(f"[bootstrap] metrics service {os.environ.get('METRICS_CH_HOST')}")
        metrics = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    else:
        print("[bootstrap] metrics service is same as target — reusing client")
        metrics = target
    apply_all(metrics, METRICS_DDL)

    print("[bootstrap] done")


if __name__ == "__main__":
    main()
