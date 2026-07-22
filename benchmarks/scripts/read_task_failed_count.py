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
"""Read back this run's task_failed_count from perf.metrics.

10_insert_from_event_log.sql records task_failed_count (count of non-Success
Spark task attempts) into perf.metrics as part of the gated capture step. It
feeds the validity-guard flagging (plan §6.10, contract §1.3 token
`task_retries`): a run with retried task attempts is non-comparable because
retried work pollutes throughput/volume accounting.

This is the read side of the flag decision: the flag must be decided AFTER the
fact exists, and task_failed_count only exists as a perf.metrics row once
capture has run. We echo the integer to stdout so the workflow can capture it
into GITHUB_ENV (TASK_FAILED_COUNT) and let the RUNTIME jq assembly derive
runtime['flagged'] / runtime['flag_reason'] before insert_run_record.py runs.

Cheap by construction: one point query by run_id for a single metric row. If the
row is absent (older capture, or the metric was not emitted) we print 0 — a
missing task_failed_count is treated as "no retries observed", never as a flag.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD, RUN_ID
"""
import ch_common


def main() -> None:
    run_id = ch_common.require("RUN_ID")
    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")

    rows = client.query(
        "SELECT value FROM perf.metrics "
        "WHERE run_id = {run_id:String} AND metric_name = 'task_failed_count' "
        "LIMIT 1",
        parameters={"run_id": run_id},
    ).result_rows

    value = int(rows[0][0]) if rows else 0
    # Single integer on stdout, nothing else — the workflow captures it verbatim.
    print(value)


if __name__ == "__main__":
    main()
