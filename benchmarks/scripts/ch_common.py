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
import os
import sys

import clickhouse_connect


def require(name: str) -> str:
    if name not in os.environ:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(1)
    return os.environ[name]


def get_client(host_var: str, user_var: str, password_var: str):
    return clickhouse_connect.get_client(
        host=require(host_var),
        port=8443,
        username=require(user_var),
        password=require(password_var),
        secure=True,
    )
