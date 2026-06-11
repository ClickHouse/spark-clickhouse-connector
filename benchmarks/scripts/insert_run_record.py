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
              RUN_ID, RUN_START, RUN_END, GIT_SHA, EMR_RELEASE_LABEL,
              CONNECTOR_VERSION
Optional env: CONNECTOR (default 'spark'), RUN_PROFILE (default '')
"""
import os

import ch_common


def main() -> None:
    target = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")
    clickhouse_version = target.query("SELECT version()").result_rows[0][0]

    metrics = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    metrics.command(
        """
        INSERT INTO perf.runs
          (run_id, run_started_at, run_ended_at, git_sha,
           connector, run_profile,
           spark_version, scala_version, connector_version,
           clickhouse_version, emr_release)
        VALUES
          ({run_id:String},
           parseDateTimeBestEffort({run_start:String}),
           parseDateTimeBestEffort({run_end:String}),
           {git_sha:String},
           {connector:String}, {run_profile:String},
           '3.5', '2.12', {connector_version:String},
           {clickhouse_version:String}, {emr_release:String})
        """,
        parameters={
            "run_id": ch_common.require("RUN_ID"),
            "run_start": ch_common.require("RUN_START"),
            "run_end": ch_common.require("RUN_END"),
            "git_sha": ch_common.require("GIT_SHA"),
            "connector": os.environ.get("CONNECTOR", "spark"),
            "run_profile": os.environ.get("RUN_PROFILE", ""),
            "connector_version": ch_common.require("CONNECTOR_VERSION"),
            "clickhouse_version": clickhouse_version,
            "emr_release": ch_common.require("EMR_RELEASE_LABEL"),
        },
    )
    print(f"inserted run record {ch_common.require('RUN_ID')} (CH {clickhouse_version})")


if __name__ == "__main__":
    main()
