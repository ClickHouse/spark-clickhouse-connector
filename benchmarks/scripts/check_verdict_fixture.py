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
"""Contract §3 verdict truth-table acceptance runner.

The contract (docs/benchmark-v2-contract.md §3, "Acceptance rule (PINNED)")
requires that any artifact emitting a VERDICT be accepted through fixture rows
asserted THROUGH THE REAL dataset SQL. This runner:

  1. seeds the synthetic PAIR rows (benchmarks/sql/perf/90_verdict_fixture_seed.sql)
     under the reserved fixture identity (connector='verdict_fixture',
     run_ids 'FIXTURE-*'),
  2. runs benchmarks/dashboard/v2/v_verdict_fixture_check.sql, which computes the
     PINNED ratio->verdict map over those rows THROUGH v_pair_ratios' verbatim
     ratio CTE, emitting (pair_id, tier, metric, direction, ratio, flagged,
     actual_verdict, expected_verdict, pass),
  3. asserts EVERY cell has actual == expected, prints the full pass table, and
     exits NON-ZERO listing any cell whose actual verdict != expected.

DWH-vs-perf NAMING (why local-fixture mode is the PRIMARY / recommended path):
  The production v2 consumer views read raw_connectors_load_testing.* — that is
  the DWH MIRROR of the metrics service's perf.* tables, kept in sync by a
  ClickPipe. The fixture seed writes to perf.* on the metrics service. So an
  acceptance run against the DWH would have to WAIT for the ClickPipe to mirror
  every fixture row before the (DWH-reading) views could see them — a slow,
  flaky, CI-hostile round-trip. v_verdict_fixture_check.sql therefore reads
  perf.* DIRECTLY (NOT the mirror), and this runner's PRIMARY mode is LOCAL:
  create perf.* + seed + view + assert all inside a single `clickhouse local`
  process. That proves the REAL view SQL files produce the pinned verdicts with
  no external service and no mirroring, which is exactly what CI needs.

MODES:
  local  (default, recommended): everything in `clickhouse local`. No env, no
          network. Creates perf.{runs,metrics,ch_inserts} from the repo DDL,
          seeds, runs the check view, asserts. This is what CI runs.
  remote: run seed + check against a live metrics service's perf.* via
          clickhouse_connect (ch_common env: METRICS_CH_HOST / METRICS_CH_USER /
          METRICS_CH_PASSWORD). Use to verify the fixture on the real perf.*
          (the seed lands there; the check view reads perf.* directly so no
          mirror wait). Off by default — the seed MUTATES perf.* (scoped to the
          fixture identity, idempotent) so it is opt-in.

USAGE:
  check_verdict_fixture.py            # local mode (default)
  check_verdict_fixture.py --local    # explicit local mode
  check_verdict_fixture.py --remote   # against live perf.* via ch_common env
"""
import os
import re
import subprocess
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SEED_SQL = os.path.join(REPO_ROOT, "benchmarks", "sql", "perf",
                        "90_verdict_fixture_seed.sql")
CHECK_SQL = os.path.join(REPO_ROOT, "benchmarks", "dashboard", "v2",
                         "v_verdict_fixture_check.sql")
DDL_FILES = [
    os.path.join(REPO_ROOT, "benchmarks", "sql", "perf", "01_create_database.sql"),
    os.path.join(REPO_ROOT, "benchmarks", "sql", "perf", "02_create_runs.sql"),
    os.path.join(REPO_ROOT, "benchmarks", "sql", "perf", "03_create_metrics.sql"),
    os.path.join(REPO_ROOT, "benchmarks", "sql", "perf", "04_create_ch_inserts.sql"),
]

# The check view SELECT columns, in order (kept aligned with the view's final
# SELECT so we can index rows positionally regardless of client).
COLS = ["pair_id", "tier", "metric", "direction", "ratio", "flagged",
        "actual_verdict", "expected_verdict", "pass"]


def _strip_sql_comments(sql: str) -> str:
    # Drop -- line comments so we can split on ';' without a comment eating one.
    return re.sub(r"--[^\n]*", "", sql)


def _split_statements(sql: str):
    body = _strip_sql_comments(sql)
    return [s.strip() for s in body.split(";") if s.strip()]


# ----------------------------------------------------------------------------
# LOCAL mode — one `clickhouse local` process, TSV out.
# ----------------------------------------------------------------------------
def _clickhouse_local_binary():
    # `clickhouse local` (space) is the modern invocation; `clickhouse-local`
    # (hyphen) is the legacy binary name. Prefer whichever resolves.
    for cand in (["clickhouse", "local"], ["clickhouse-local"]):
        try:
            subprocess.run(cand + ["--version"], capture_output=True, check=True)
            return cand
        except (OSError, subprocess.CalledProcessError):
            continue
    sys.exit("ERROR: neither `clickhouse local` nor `clickhouse-local` is available")


def run_local():
    binary = _clickhouse_local_binary()
    # A single --multiquery script: DDL, seed, create the check VIEW, then SELECT
    # it as TSVWithNames. clickhouse-local keeps state across statements in one
    # process, so the perf.* tables the seed writes are visible to the view.
    ddl = "\n".join(open(f).read() for f in DDL_FILES)
    seed = open(SEED_SQL).read()
    check = open(CHECK_SQL).read().rstrip().rstrip(";")

    script_parts = [ddl, seed,
                    "CREATE VIEW perf.v_verdict_fixture_check AS\n" + check,
                    "SELECT " + ", ".join(COLS)
                    + " FROM perf.v_verdict_fixture_check "
                      "ORDER BY pair_id, metric FORMAT TSVWithNames;"]
    script = "\n;\n".join(script_parts)

    proc = subprocess.run(
        binary + ["--multiquery",
                  # lightweight DELETE in the seed needs mutations to apply
                  # synchronously so the subsequent view sees the cleared state.
                  "--mutations_sync=1"],
        input=script, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        sys.exit(f"ERROR: clickhouse local failed (exit {proc.returncode})")
    return _parse_tsv(proc.stdout)


def _parse_tsv(text: str):
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines:
        return []
    header = lines[0].split("\t")
    rows = []
    for ln in lines[1:]:
        cells = ln.split("\t")
        rows.append(dict(zip(header, cells)))
    return rows


# ----------------------------------------------------------------------------
# REMOTE mode — live perf.* via clickhouse_connect (ch_common env).
# ----------------------------------------------------------------------------
def run_remote():
    import ch_common  # local import so local mode needs no clickhouse_connect
    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER",
                                  "METRICS_CH_PASSWORD")
    # Seed is multi-statement (DELETE + INSERTs); clickhouse_connect.command runs
    # one statement at a time, so split on ';'.
    for stmt in _split_statements(open(SEED_SQL).read()):
        client.command(stmt, settings={"mutations_sync": 1})
    check = open(CHECK_SQL).read().rstrip().rstrip(";")
    result = client.query(check)
    names = result.column_names
    return [dict(zip(names, [_str(v) for v in row])) for row in result.result_rows]


def _str(v):
    if v is None:
        return r"\N"
    if isinstance(v, bool):
        return "1" if v else "0"
    return str(v)


# ----------------------------------------------------------------------------
# Assertion + reporting.
# ----------------------------------------------------------------------------
# 16 pairs × {throughput_rows_per_sec (HB banded ±9%),
#             cpu_seconds_per_Mrows (LB banded ±6%),
#             parts_per_insert (TRIPWIRE)} = 48 cells (contract §3, Amendment
# 2026-07-09b). Covers {NULL(pinned-absent),NULL(head-absent),0-denom,below,in,
# above,near-edge} × {HB,LB} banded × {flagged,unflagged}, PLUS the parts TRIPWIRE
# axis {OK(==1.0), fired(!=1.0), head-absent(NO_DATA)} × {flagged (armed),
# unflagged}. The head-absent pair (P16, +3 cells) is the kafka cross-check gap:
# the head-driven join used to DROP an absent-head metric so the contract map's
# "NULL/absent => NO_DATA" could never render in the Spark artifact.
# merge_amplification is WATCH-ONLY (not gated) and is intentionally NOT asserted.
EXPECTED_CELL_COUNT = 48


def _truthy(v: str) -> bool:
    return str(v).strip() in ("1", "true", "True")


def report_and_exit(rows):
    if not rows:
        sys.exit("ERROR: fixture check returned ZERO rows — seed did not land or "
                 f"the view produced nothing. Expected {EXPECTED_CELL_COUNT} cells.")

    # Pretty table.
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in COLS}
    header = "  ".join(c.ljust(widths[c]) for c in COLS)
    print(header)
    print("  ".join("-" * widths[c] for c in COLS))
    for r in rows:
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in COLS))

    failures = [r for r in rows if not _truthy(r.get("pass", "0"))]
    unexpected = [r for r in rows if r.get("expected_verdict") == "UNEXPECTED-CELL"]
    print()
    print(f"cells: {len(rows)} (expected {EXPECTED_CELL_COUNT}), "
          f"failures: {len(failures)}")

    problems = []
    if len(rows) != EXPECTED_CELL_COUNT:
        problems.append(f"cell COUNT {len(rows)} != expected {EXPECTED_CELL_COUNT} "
                        f"(coverage matrix incomplete)")
    for r in unexpected:
        problems.append(f"{r.get('pair_id')}/{r.get('metric')}: hit "
                        f"UNEXPECTED-CELL (matrix/oracle mismatch)")
    for r in failures:
        problems.append(f"{r.get('pair_id')}/{r.get('metric')}: "
                        f"actual={r.get('actual_verdict')} != "
                        f"expected={r.get('expected_verdict')} "
                        f"(ratio={r.get('ratio')}, flagged={r.get('flagged')})")

    if problems:
        print("\nFAIL — contract §3 acceptance NOT met:")
        for p in problems:
            print(f"  * {p}")
        sys.exit(1)

    print("\nPASS — all fixture cells match the pinned ratio->verdict map "
          "(contract §3).")


def main():
    mode = "local"
    for a in sys.argv[1:]:
        if a == "--remote":
            mode = "remote"
        elif a in ("--local", "-l"):
            mode = "local"
        elif a in ("-h", "--help"):
            print(__doc__)
            return
        else:
            sys.exit(f"unknown argument: {a}")

    print(f"[check_verdict_fixture] mode={mode}")
    rows = run_remote() if mode == "remote" else run_local()
    report_and_exit(rows)


if __name__ == "__main__":
    main()
