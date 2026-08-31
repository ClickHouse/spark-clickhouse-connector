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
              RUN_ID, RUN_START, RUN_END, GIT_SHA, CONNECTOR_VERSION
Optional env: CONNECTOR (default 'spark'), RUN_PROFILE (default ''),
              RUNTIME (JSON object of connector/runtime attributes written
              verbatim into perf.runs.runtime Map(String,String)). The workflow
              populates it with connector/EMR attrs, the operating config under
              test (contract §1.4 shared keys: batch_size, write_parallelism,
              async_insert, partition_scheme, dataset; write_parallelism is the
              OBSERVED write-stage partition count reported by the ingest job's
              S3 sidecar — record-only, nothing forces it — or the fallback
              token 'input-splits' when the sidecar is missing) and the
              mandatory scope keys (contract §1.1: environment_class,
              target_region), e.g.
              {"spark_version":"3.5","scala_version":"2.12",
               "emr_release":"emr-7.13.0","batch_size":"100000",
               "write_parallelism":"147","async_insert":"0",
               "partition_scheme":"toYear(EventDate)","dataset":"hits",
               "environment_class":"production","target_region":"us-east-1"}
"""
import json
import os

import ch_common


def main() -> None:
    target = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")
    clickhouse_version = target.query("SELECT version()").result_rows[0][0]

    # Connector-specific runtime attributes go into a generic Map column, so
    # adding a connector (kafka, ...) never needs a schema change.
    runtime = json.loads(os.environ.get("RUNTIME", "{}"))

    metrics = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    metrics.command(
        """
        INSERT INTO perf.runs
          (run_id, run_started_at, run_ended_at, git_sha,
           connector, run_profile, connector_version,
           clickhouse_version, runtime)
        VALUES
          ({run_id:String},
           parseDateTimeBestEffort({run_start:String}),
           parseDateTimeBestEffort({run_end:String}),
           {git_sha:String},
           {connector:String}, {run_profile:String}, {connector_version:String},
           {clickhouse_version:String},
           mapFromArrays({runtime_keys:Array(String)}, {runtime_values:Array(String)}))
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
            "runtime_keys": list(runtime.keys()),
            "runtime_values": [str(v) for v in runtime.values()],
        },
    )
    print(f"inserted run record {ch_common.require('RUN_ID')} (CH {clickhouse_version})")


if __name__ == "__main__":
    main()
