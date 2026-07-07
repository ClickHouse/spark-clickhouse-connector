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


def apply_all(client, ddl_files):
    for path in ddl_files:
        rel = path.split("/sql/", 1)[-1]
        print(f"  applying {rel}")
        client.command(open(path).read())


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
