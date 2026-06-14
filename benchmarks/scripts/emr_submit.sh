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
# Submit the PySpark script as a Spark step and wait for it to finish.
#
# Required env: AWS_REGION, CLUSTER_ID, JAR_S3_URI, SCRIPT_S3_URI, RUN_ID,
#               EVENT_LOG_DIR_SPARK,
#               CH_HOST, CH_PORT, CH_PROTOCOL, CH_USER, CH_SECRET_ID,
#               CH_DATABASE, CH_TABLE, INPUT_PARQUET_GLOB
set -euo pipefail


_ev_path="${EVENT_LOG_DIR_SPARK#s3a://}"
aws s3api put-object --bucket "${_ev_path%%/*}" --key "${_ev_path#*/}.keep" >/dev/null

ARGS=(
  --deploy-mode cluster
  --jars "$JAR_S3_URI"
  --conf spark.eventLog.enabled=true
  --conf "spark.eventLog.dir=${EVENT_LOG_DIR_SPARK}"
  --conf spark.eventLog.logStageExecutorMetrics=true
  --conf spark.executor.metrics.pollingInterval=10000
  --conf spark.eventLog.compress=false
  --conf "spark.yarn.appMasterEnv.CH_HOST=${CH_HOST}"
  --conf "spark.yarn.appMasterEnv.CH_PORT=${CH_PORT}"
  --conf "spark.yarn.appMasterEnv.CH_PROTOCOL=${CH_PROTOCOL}"
  --conf "spark.yarn.appMasterEnv.CH_USER=${CH_USER}"
  --conf "spark.yarn.appMasterEnv.CH_SECRET_ID=${CH_SECRET_ID}"
  --conf "spark.yarn.appMasterEnv.AWS_REGION=${AWS_REGION}"
  --conf "spark.yarn.appMasterEnv.CH_DATABASE=${CH_DATABASE}"
  --conf "spark.yarn.appMasterEnv.CH_TABLE=${CH_TABLE}"
  --conf "spark.yarn.appMasterEnv.INPUT_PARQUET_GLOB=${INPUT_PARQUET_GLOB}"
  --conf "spark.yarn.appMasterEnv.RUN_ID=${RUN_ID}"
  "$SCRIPT_S3_URI"
)

# One raw line per arg -> jq's [inputs] collects them into the Args array,
# so values with commas/quotes (e.g. CH_PASSWORD) survive intact. (Raw input,
# not jq --args, because --args still option-parses values starting with '--'.)
steps_json=$(printf '%s\n' "${ARGS[@]}" | jq -nR '[{
  Type: "Spark",
  Name: "ClickBenchIngest",
  ActionOnFailure: "TERMINATE_CLUSTER",
  Args: [inputs]
}]')

STEP_ID=$(aws emr add-steps \
  --region "$AWS_REGION" \
  --cluster-id "$CLUSTER_ID" \
  --steps "$steps_json" \
  --query 'StepIds[0]' --output text)

echo "submitted step $STEP_ID, waiting..."
aws emr wait step-complete --region "$AWS_REGION" --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID"

STATE=$(aws emr describe-step --region "$AWS_REGION" --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID" \
  --query 'Step.Status.State' --output text)
echo "final state: $STATE"
test "$STATE" = "COMPLETED"
