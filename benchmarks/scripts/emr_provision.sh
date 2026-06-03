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
# Provisions a transient EMR cluster. Prints the cluster_id on stdout.
#
# Required env: AWS_REGION, EMR_RELEASE_LABEL, EMR_LOG_URI, EMR_SUBNET_ID,
#               EMR_EC2_KEY_NAME, EMR_INSTANCE_TYPE, EMR_CORE_COUNT,
#               EMR_SERVICE_ROLE, EMR_INSTANCE_PROFILE, RUN_ID
set -euo pipefail

CLUSTER_ID=$(aws emr create-cluster \
  --region "$AWS_REGION" \
  --name "clickbench-load-test-${RUN_ID}" \
  --release-label "$EMR_RELEASE_LABEL" \
  --applications Name=Spark \
  --log-uri "$EMR_LOG_URI" \
  --service-role "$EMR_SERVICE_ROLE" \
  --ec2-attributes "{\"KeyName\":\"${EMR_EC2_KEY_NAME}\",\"SubnetId\":\"${EMR_SUBNET_ID}\",\"InstanceProfile\":\"${EMR_INSTANCE_PROFILE}\"}" \
  --instance-groups "[\
    {\"Name\":\"Master\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${EMR_INSTANCE_TYPE}\",\"InstanceCount\":1},\
    {\"Name\":\"Core\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${EMR_INSTANCE_TYPE}\",\"InstanceCount\":${EMR_CORE_COUNT}}\
  ]" \
  --visible-to-all-users \
  --auto-terminate \
  --query 'ClusterId' --output text)

echo "$CLUSTER_ID"
