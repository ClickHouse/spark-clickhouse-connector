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
"""Compute this run's EMR cost and record it as the run_cost_usd metric (plan §6.6).

cost = total_instances x cluster_hours x (ec2_on_demand + emr_uplift) per hour

where total_instances = 1 master + EMR_CORE_COUNT cores (all EMR_INSTANCE_TYPE,
per emr_provision.sh) and cluster_hours is approximated by RUN_END - RUN_START
(build + provision + ingest window; teardown is quick and EMR auto-terminates).

Prices come from a small in-repo us-east-1 on-demand table below — NO AWS Pricing
API call at run time (per plan §6.6: no new AWS calls). Prices drift slowly; bump
the table deliberately when they change. The EMR uplift is the per-instance-hour
EMR surcharge on top of the EC2 on-demand rate.

Makes cadence/tier decisions data-driven and 30-day cost aggregable on the
dashboard headline.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD, RUN_ID,
              RUN_START, RUN_END, EMR_INSTANCE_TYPE, EMR_CORE_COUNT
"""
import os
import sys
from datetime import datetime

import ch_common

# us-east-1 on-demand USD/hour: (ec2_on_demand, emr_uplift). Keep in sync with the
# instance types the workflow's run profiles use (see "Resolve cluster specs").
PRICES = {
    "m6i.2xlarge": (0.384, 0.096),   # small profile
    "m6i.4xlarge": (0.768, 0.192),   # large profile
}


def parse_ts(name: str) -> datetime:
    # RUN_START/RUN_END are ISO-8601 UTC, e.g. 2026-06-29T06:12:34.
    return datetime.strptime(ch_common.require(name)[:19], "%Y-%m-%dT%H:%M:%S")


def main() -> None:
    run_id = ch_common.require("RUN_ID")
    instance_type = ch_common.require("EMR_INSTANCE_TYPE")
    core_count = int(ch_common.require("EMR_CORE_COUNT"))

    if instance_type not in PRICES:
        print(
            f"ERROR: no price entry for instance type '{instance_type}'. "
            f"Add it to PRICES in {os.path.basename(__file__)}.",
            file=sys.stderr,
        )
        sys.exit(1)
    ec2_rate, emr_rate = PRICES[instance_type]

    hours = (parse_ts("RUN_END") - parse_ts("RUN_START")).total_seconds() / 3600.0
    total_instances = core_count + 1  # + master
    cost = total_instances * hours * (ec2_rate + emr_rate)

    print(f"run_cost_usd for {run_id}: {total_instances} x {instance_type} "
          f"for {hours:.3f}h @ ${ec2_rate + emr_rate:.3f}/h = ${cost:.4f}")

    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    client.command(
        "INSERT INTO perf.metrics (run_id, metric_name, unit, value) "
        "VALUES ({run_id:String}, 'run_cost_usd', 'usd', {cost:Float64})",
        parameters={"run_id": run_id, "cost": cost},
    )


if __name__ == "__main__":
    main()
