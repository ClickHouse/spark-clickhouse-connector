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
"""Poll active part count on the target CH until background merges settle.

After the Spark ingest finishes, ClickHouse keeps merging the small parts in
the background for minutes-to-hours. This waits until the active part count is
stable AND no merges are in flight (for STABLE_SAMPLES consecutive polls) or
SETTLE_TIMEOUT elapses, then prints the wall-clock timestamp (ISO 8601 UTC) at
which it settled.

Contract: the settle-end timestamp is the ONLY thing written to stdout, so the
workflow can capture it as `SETTLE_END=$(wait_for_settle.py)`. All progress
logging goes to stderr.

Required env: TARGET_CH_HOST, TARGET_CH_USER, TARGET_CH_PASSWORD,
              CH_DATABASE, CH_TABLE
Optional env: POLL_INTERVAL (default 10s), STABLE_SAMPLES (default 3),
              SETTLE_TIMEOUT (default 1800s)
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
    settle_timeout = int(os.environ.get("SETTLE_TIMEOUT", "1800"))

    client = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")

    start = time.monotonic()
    prev = -1
    stable = 0

    while True:
        if time.monotonic() - start > settle_timeout:
            log(f"settle timeout ({settle_timeout}s) hit; proceeding")
            break

        parts = client.query(
            "SELECT count() FROM system.parts "
            "WHERE database = {db:String} AND table = {tbl:String} AND active",
            parameters={"db": db, "tbl": table},
        ).result_rows[0][0]

        merges = client.query(
            "SELECT count() FROM system.merges "
            "WHERE database = {db:String} AND table = {tbl:String}",
            parameters={"db": db, "tbl": table},
        ).result_rows[0][0]

        # Settled = the active part count stopped changing AND no merges are in
        # flight. Keying off system.merges (instead of waiting for the part
        # count to visibly drop) handles both failure modes: merges still
        # running keep us waiting even through a momentary plateau, and a small
        # run whose merges finished before the first poll settles promptly
        # instead of burning the full SETTLE_TIMEOUT.
        if parts == prev and merges == 0:
            stable += 1
        else:
            stable = 0
        log(f"active parts: {parts}, in-flight merges: {merges} "
            f"(stable {stable}/{stable_samples})")

        if stable >= stable_samples:
            log("merges settled")
            break

        prev = parts
        time.sleep(poll_interval)

    print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))


if __name__ == "__main__":
    main()
