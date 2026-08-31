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
"""Read back this run's integrity verdict and fail the job on a mismatch.

20_insert_integrity.sql records rows_delivered / rows_expected / duplicate_rows /
integrity_ok into perf.metrics as part of the (gated, atomic) capture step. This
step runs AFTER capture + run-record + DWH export so the evidence is already
persisted and exported before we decide the run's fate: per plan §6.10 an
integrity mismatch FAILS the run outright (unlike the flagged-not-failed guards),
but the metrics must survive for the dashboard, so the failure lives here rather
than in a rollback-triggering capture failure.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD, RUN_ID
"""
import sys

import ch_common


def main() -> None:
    run_id = ch_common.require("RUN_ID")
    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")

    rows = client.query(
        "SELECT metric_name, value FROM perf.metrics "
        "WHERE run_id = {run_id:String} "
        "AND metric_name IN ('integrity_ok', 'rows_delivered', 'rows_expected', "
        "'unique_delivered', 'unique_expected', 'duplicate_rows', "
        "'content_checksum_delivered', 'content_checksum_expected', 'content_checksum_ok')",
        parameters={"run_id": run_id},
    ).result_rows
    m = {name: value for name, value in rows}

    if "integrity_ok" not in m:
        print(f"ERROR: no integrity metrics found for run {run_id}", file=sys.stderr)
        sys.exit(1)

    delivered = m.get("rows_delivered")
    expected = m.get("rows_expected")
    uniq_delivered = m.get("unique_delivered")
    uniq_expected = m.get("unique_expected")
    dups = m.get("duplicate_rows")
    # Content checksum: enforced inside integrity_ok only once SOURCE_CONTENT_CHECKSUM
    # is pinned; content_checksum_ok is 1 (no-op) until then. Surfaced for diagnostics.
    ck_ok = m.get("content_checksum_ok")
    ck_del = m.get("content_checksum_delivered")
    ck_exp = m.get("content_checksum_expected")
    ck_state = ("disabled (no source constant pinned)"
                if ck_exp is None or float(ck_exp) <= 0
                else f"delivered={ck_del} expected={ck_exp} ok={ck_ok}")
    print(f"integrity for {run_id}: rows delivered={delivered} expected={expected} "
          f"| unique delivered={uniq_delivered} expected={uniq_expected} "
          f"| duplicate_rows={dups} | content_checksum: {ck_state} "
          f"| integrity_ok={m['integrity_ok']}")

    # Fail-closed: integrity_ok is toFloat64(bool) — exactly 1.0 on pass, 0.0 on any
    # mismatch. Treat anything not clearly "pass" as a failure rather than relying on
    # exact float equality, so future drift in how the column is written can't slip a
    # non-1.0 value through the gate.
    if float(m["integrity_ok"]) < 0.5:
        print(
            f"ERROR: integrity check FAILED for {run_id} "
            f"(rows delivered={delivered}/expected={expected}, "
            f"unique delivered={uniq_delivered}/expected={uniq_expected}, "
            f"duplicate_rows={dups})",
            file=sys.stderr,
        )
        sys.exit(1)

    print("integrity OK")


if __name__ == "__main__":
    main()
