#!/usr/bin/env bash
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
# One-time Superset init: admin user (local-only admin/admin), schema, roles.
set -euo pipefail
C=clickbench-superset
docker exec "$C" superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@clickbench.local --password admin || true
docker exec "$C" superset db upgrade
docker exec "$C" superset init
echo "Superset ready at http://localhost:8088  (admin / admin)"
