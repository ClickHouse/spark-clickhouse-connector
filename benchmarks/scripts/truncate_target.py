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
"""Truncate the target table before a benchmark run (clickhouse-connect, HTTPS).

Assumes the service is already awake (bootstrap_schema.py runs first in the
workflow). Set SKIP_TRUNCATE=1 to bypass (accumulate-rows experiments).

Required env: TARGET_CH_HOST, TARGET_CH_USER, TARGET_CH_PASSWORD,
              CH_DATABASE, CH_TABLE
"""
import os
import sys

import ch_common


def main() -> None:
    if os.environ.get("SKIP_TRUNCATE", "0") == "1":
        print(f"SKIP_TRUNCATE=1 set; leaving {os.environ.get('CH_DATABASE')}."
              f"{os.environ.get('CH_TABLE')} as-is")
        return

    db = ch_common.require("CH_DATABASE")
    table = ch_common.require("CH_TABLE")
    client = ch_common.get_client("TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD")

    before = client.query(f"SELECT count() FROM {db}.{table}").result_rows[0][0]
    print(f"rows in {db}.{table} before truncate: {before}")

    client.command(f"TRUNCATE TABLE {db}.{table}")

    after = client.query(f"SELECT count() FROM {db}.{table}").result_rows[0][0]
    print(f"rows after truncate: {after}")
    if after != 0:
        print("ERROR: truncate didn't zero the table", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
