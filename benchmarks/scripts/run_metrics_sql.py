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
              RUN_ID, RUN_START, RUN_END, TARGET_CH_HOST, TARGET_CH_USER,
              TARGET_CH_PASSWORD, CH_DATABASE, CH_TABLE
Optional env: SETTLE_END (defaults to RUN_END), SETTLE_SECONDS (default 0),
              SETTLE_TIMED_OUT (default 0), INPUT_PARQUET_GLOB (source glob,
              s3a:// or s3://; exposed to SQL as {source_glob} in s3:// form),
              EVENT_LOG_URI, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
              AWS_SESSION_TOKEN
"""
import os
import sys

import ch_common


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: run_metrics_sql.py <sql-file>")
    sql = open(sys.argv[1]).read()

    db = os.environ.get("CH_DATABASE", "")
    table = os.environ.get("CH_TABLE", "")
    # ClickHouse's s3() wants the s3:// scheme; the ingest glob is s3a:// (Spark).
    source_glob = os.environ.get("INPUT_PARQUET_GLOB", "")
    if source_glob.startswith("s3a://"):
        source_glob = "s3://" + source_glob[len("s3a://"):]
    parameters = {
        "run_id": ch_common.require("RUN_ID"),
        "run_start": ch_common.require("RUN_START"),
        "run_end": ch_common.require("RUN_END"),
        "settle_end": os.environ.get("SETTLE_END") or os.environ.get("RUN_END", ""),
        "settle_seconds": float(os.environ.get("SETTLE_SECONDS", "0")),
        "settle_timed_out": float(os.environ.get("SETTLE_TIMED_OUT", "0")),
        "source_glob": source_glob,
        "event_log_uri": os.environ.get("EVENT_LOG_URI", ""),
        "aws_access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN", ""),
        # remoteSecure() reaches the target over native-secure 9440 regardless
        # of how we (the client) connect to the metrics service.
        "target_addr": f'{ch_common.require("TARGET_CH_HOST")}:9440',
        "target_user": os.environ.get("TARGET_CH_USER", ""),
        "target_password": os.environ.get("TARGET_CH_PASSWORD", ""),
        "ch_database": db,
        "ch_table": table,
        "table_qualified": f"{db}.{table}",
    }

    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    client.command(sql, parameters=parameters)


if __name__ == "__main__":
    main()
