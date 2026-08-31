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
"""Roll back this run's rows when it wasn't fully recorded FOR ITS OUTCOME CLASS.

Metrics capture is several independent INSERTs into perf.metrics / perf.ch_inserts,
and the run record (perf.runs) is a separate step. This deletes the run's rows
from all three perf tables (including any pre-run covariate rows written before
submit) so the source never holds an incomplete artifact.

OUTCOME-CLASS INVARIANT (task #6): a run has an outcome class — 'success' or
'failed' (runtime['outcome']). A run's rows are COMPLETE FOR ITS CLASS when that
class's capture families + flag decision + run record all landed. This rollback
fires ONLY when that artifact BREAKS (capture partial, flag decision failed, or
run-record insert failed) — in EITHER class. It MUST NOT fire merely because the
run failed: a failed-class run whose reduced capture + flags + run record all
succeeded is a complete, marked artifact and is kept and exported. The caller
(the run-arm composite action) encodes exactly this gating; this script is the
unconditional delete it invokes.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD, RUN_ID
"""
import ch_common


def main() -> None:
    run_id = ch_common.require("RUN_ID")
    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    for table in ("runs", "metrics", "ch_inserts"):
        client.command(
            f"ALTER TABLE perf.{table} DELETE WHERE run_id = {{run_id:String}}",
            parameters={"run_id": run_id},
            settings={"mutations_sync": 1},
        )
        print(f"rolled back partial perf.{table} rows for {run_id}")


if __name__ == "__main__":
    main()
