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
"""Pipeline freshness watchdog (board task #39).

Answers "did the expected nightly pair actually land?" — the blind spot the
dashboards can't show: a run that never happened leaves no row to look at. Runs
the two freshness conditions (semantics shared with
benchmarks/dashboard/v2/alerts/missing_pair_freshness.sql, which is the Superset
variant against the DWH mirror) against perf.runs on the metrics service:

  ABSENT     no runs at all for the connector (pipeline never produced anything).
  STALE      the newest run is older than the freshness SLA (default 36h) — the
             two-arm pipeline has not produced a run recently.
  HALF_PAIR  a pair_id inside the window has one arm but not both for a tier — the
             run half-completed, so v2_pair_ratios (INNER JOIN) silently emits no
             ratio and a missing arm looks identical to "no regression" on Tab 1.

Exit 0 = healthy (nothing fired). Exit 2 = one or more conditions fired (the
caller — the scheduled workflow — turns a non-zero exit into an alert). Exit 1 =
the check itself could not run (config/connection error), which is also worth an
alert but is distinguished from a real freshness miss.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD
Optional env: FRESHNESS_CONNECTOR (default 'spark'),
              FRESHNESS_SLA_HOURS  (default '36')
"""
import os
import sys

import ch_common


def main() -> None:
    connector = os.environ.get("FRESHNESS_CONNECTOR", "spark")
    try:
        sla_hours = int(os.environ.get("FRESHNESS_SLA_HOURS", "36"))
    except ValueError:
        print("ERROR: FRESHNESS_SLA_HOURS must be an integer", file=sys.stderr)
        sys.exit(1)

    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    params = {"connector": connector, "sla": sla_hours}

    # (A) ABSENT / STALE — newest run age for the connector.
    total, last_at, age_h = client.query(
        "SELECT count() AS n, max(run_started_at) AS last_at, "
        "toInt32(round(dateDiff('hour', max(run_started_at), now()))) AS age_h "
        "FROM perf.runs WHERE connector = {connector:String}",
        parameters=params,
    ).result_rows[0]

    # (B) HALF_PAIR — a pair inside the window missing an arm for some tier.
    half_pairs = client.query(
        "SELECT pair_id, tier, arrayStringConcat(groupUniqArray(arm), ',') AS arms "
        "FROM ("
        "  SELECT runtime['pair_id'] AS pair_id, run_started_at, "
        "         coalesce(nullIf(runtime['arm'], ''),  'head') AS arm, "
        "         coalesce(nullIf(runtime['tier'], ''), '1')    AS tier, "
        "         coalesce(nullIf(runtime['outcome'], ''), 'success') AS outcome "
        "  FROM perf.runs WHERE connector = {connector:String}"
        ") "
        "WHERE pair_id != '' AND outcome != 'failed' "
        "  AND run_started_at >= now() - toIntervalHour({sla:UInt32}) "
        "GROUP BY pair_id, tier HAVING uniqExact(arm) < 2 "
        "ORDER BY pair_id, tier",
        parameters=params,
    ).result_rows

    problems = []
    if total == 0:
        problems.append(f"ABSENT: no '{connector}' runs exist at all — the pipeline has never produced a run.")
    elif age_h is not None and age_h > sla_hours:
        problems.append(
            f"STALE: newest '{connector}' run is {age_h}h old (SLA {sla_hours}h), "
            f"last at {last_at} — the two-arm pipeline may be down.")
    for pair_id, tier, arms in half_pairs:
        problems.append(
            f"HALF_PAIR: pair {pair_id} tier {tier} has arms [{arms}] — expected "
            f"head+pinned; v2_pair_ratios will emit no ratio for it.")

    if not problems:
        print(f"freshness OK: '{connector}' newest run {age_h}h old "
              f"(SLA {sla_hours}h), no half-pairs in window.")
        sys.exit(0)

    print(f"FRESHNESS ALERT for '{connector}' (SLA {sla_hours}h):", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
