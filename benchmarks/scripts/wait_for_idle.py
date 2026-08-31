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
"""Poll the target CH until it is IDLE, BEFORE the arm's timed window starts.

The HEAD/pinned ratio cancels the AVERAGE environment but not DRIFT across the
four sequential ingests in a pair (arm1-t0, arm1-t1, arm2-t0, arm2-t1). The
benchmark already waits for merges to SETTLE *after* each Tier-1 ingest
(wait_for_settle.py). This is the symmetric PRE-ARM idle gate: it makes each arm
start from the same quiesced target state — background merges quiesced AND
resident memory flat — so a busy tail from the previous arm does not bias the
next one. It runs OUTSIDE the timed window (before RUN_START).

Idle = for STABLE_SAMPLES consecutive polls BOTH hold:
  * in-flight merges on the target table <= IDLE_MAX_MERGES, and
  * RSS (MemoryResident from system.asynchronous_metrics) is stable across
    consecutive polls, i.e. |rss - prev_rss| / prev_rss <= IDLE_RSS_TOLERANCE.
RSS is a service-wide, point-in-time gauge (same source as the pre_run_rss
covariate, SQL 21); this poller reads it DIRECTLY from the target via ch_common
(NOT remoteSecure — it connects straight to the target like wait_for_settle.py).

Contract: the idle-end timestamp (ISO 8601 UTC) is the ONLY thing written to
stdout, so the workflow can capture it as `IDLE_END=$(wait_for_idle.py)` and
compute PRE_ARM_IDLE_SECONDS. All progress logging goes to stderr.

On timeout we PROCEED anyway (never block the pair — the idle gate is a best-
effort quiescing, not a correctness gate) and, if IDLE_STATUS_FILE is set, write
a single line `1` to it; a clean idle writes `0`. The workflow reads this back
into the pre_arm_idle_timed_out flag so a censored idle wait (state never visibly
quiesced) can be flagged and excluded from trends; a stdout-only contract can't
carry a second value.

Required env: TARGET_CH_HOST, TARGET_CH_USER, TARGET_CH_PASSWORD,
              CH_DATABASE, CH_TABLE
Optional env: POLL_INTERVAL (default 10s), STABLE_SAMPLES (default 3),
              IDLE_TIMEOUT (default 600s — shorter than settle's 1800s since the
              state should already be near-quiet by pre-arm time),
              IDLE_MAX_MERGES (default 0), IDLE_RSS_TOLERANCE (default 0.02 = 2%),
              IDLE_STATUS_FILE (path to write the 1/0 timed-out flag)
"""
import os
import sys
import time
from datetime import datetime, timezone

import ch_common


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def main() -> None:
    db = ch_common.require("CH_DATABASE")
    table = ch_common.require("CH_TABLE")
    poll_interval = int(os.environ.get("POLL_INTERVAL", "10"))
    stable_samples = int(os.environ.get("STABLE_SAMPLES", "3"))
    idle_timeout = int(os.environ.get("IDLE_TIMEOUT", "600"))
    idle_max_merges = int(os.environ.get("IDLE_MAX_MERGES", "0"))
    idle_rss_tolerance = float(os.environ.get("IDLE_RSS_TOLERANCE", "0.02"))

    client = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")

    start = time.monotonic()
    prev_rss = -1.0
    stable = 0
    timed_out = False

    while True:
        if time.monotonic() - start > idle_timeout:
            log(f"idle timeout ({idle_timeout}s) hit; proceeding")
            timed_out = True
            break

        merges = client.query(
            "SELECT count() FROM system.merges "
            "WHERE database = {db:String} AND table = {tbl:String}",
            parameters={"db": db, "tbl": table},
        ).result_rows[0][0]

        # MemoryResident is a service-wide point-in-time gauge in
        # system.asynchronous_metrics (same source as the pre_run_rss covariate).
        rss = float(client.query(
            "SELECT value FROM system.asynchronous_metrics "
            "WHERE metric = 'MemoryResident'"
        ).result_rows[0][0])

        # RSS is stable when it moved less than the relative tolerance since the
        # previous poll. prev_rss < 0 is the first poll (no baseline yet) — never
        # "stable" until we have two readings to compare. Guard prev_rss == 0 so a
        # zero baseline can't divide-by-zero (treat any change off zero as moving).
        if prev_rss < 0:
            rss_stable = False
        elif prev_rss == 0:
            rss_stable = rss == 0
        else:
            rss_stable = abs(rss - prev_rss) / prev_rss <= idle_rss_tolerance

        # Idle = merges quiesced AND memory flat. Keying merges off system.merges
        # (not a part-count delta) matches wait_for_settle.py: any in-flight merge
        # above the allowance keeps us waiting, and a table that was already quiet
        # idles promptly instead of burning the full IDLE_TIMEOUT.
        if merges <= idle_max_merges and rss_stable:
            stable += 1
        else:
            stable = 0
        log(f"in-flight merges: {merges} (<= {idle_max_merges}), "
            f"rss: {rss:.0f} (stable={rss_stable}, prev={prev_rss:.0f}) "
            f"(stable {stable}/{stable_samples})")

        if stable >= stable_samples:
            log("target idle")
            break

        prev_rss = rss
        time.sleep(poll_interval)

    status_file = os.environ.get("IDLE_STATUS_FILE")
    if status_file:
        with open(status_file, "w") as f:
            f.write("1" if timed_out else "0")

    print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))


if __name__ == "__main__":
    main()
