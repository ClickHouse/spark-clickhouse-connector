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
# Idempotent: terminating an already-terminated cluster returns success.
set -euo pipefail

# CLUSTER_ID is only exported after a successful provision step, but the
# teardown workflow step runs with if:always(). If provisioning failed, no
# cluster exists and CLUSTER_ID is unset - no-op instead of tripping `set -u`.
if [[ -z "${CLUSTER_ID:-}" ]]; then
  echo "CLUSTER_ID is unset; no cluster to terminate"
  exit 0
fi

aws emr terminate-clusters --region "$AWS_REGION" --cluster-ids "$CLUSTER_ID"
echo "terminate request sent for $CLUSTER_ID"
