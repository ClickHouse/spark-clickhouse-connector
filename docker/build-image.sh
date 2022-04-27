#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Set Mirror:
# export APACHE_MIRROR=mirrors.cloud.tencent.com/apache
# export MAVEN_MIRROR=mirrors.cloud.tencent.com/maven

set -e

SELF_DIR="$(cd "$(dirname "$0")"; pwd)"

PROJECT_VERSION="$(cat "${SELF_DIR}/../version.txt")"

APACHE_MIRROR=${APACHE_MIRROR:-https://dlcdn.apache.org}
MAVEN_MIRROR=${MAVEN_MIRROR:-https://repo1.maven.org/maven2}

AWS_JAVA_SDK_VERSION=1.11.901
DELTA_VERSION=1.2.0
HADOOP_VERSION=3.3.1
HIVE_VERSION=2.3.9
ICEBERG_VERSION=0.13.1
KYUUBI_VERSION=1.5.1-incubating
POSTGRES_JDBC_VERSION=42.3.4
SCALA_BINARY_VERSION=2.12
SPARK_VERSION=3.2.1
SPARK_BINARY_VERSION=3.2

docker build \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg PROJECT_VERSION=${PROJECT_VERSION} \
  --file "${SELF_DIR}/image/scc-base.Dockerfile" \
  --tag scc-base:${PROJECT_VERSION} \
  "${SELF_DIR}/image" $@

docker build \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg PROJECT_VERSION=${PROJECT_VERSION} \
  --build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  --file "${SELF_DIR}/image/scc-hadoop.Dockerfile" \
  --tag scc-hadoop:${PROJECT_VERSION} \
  "${SELF_DIR}/image" $@

docker build \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg PROJECT_VERSION=${PROJECT_VERSION} \
  --build-arg HIVE_VERSION=${HIVE_VERSION} \
  --file "${SELF_DIR}/image/scc-metastore.Dockerfile" \
  --tag scc-metastore:${PROJECT_VERSION} \
  "${SELF_DIR}/image" $@

docker build \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg PROJECT_VERSION=${PROJECT_VERSION} \
  --build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
  --build-arg DELTA_VERSION=${DELTA_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  --build-arg ICEBERG_VERSION=${ICEBERG_VERSION} \
  --build-arg POSTGRES_JDBC_VERSION=${POSTGRES_JDBC_VERSION} \
  --build-arg SCALA_BINARY_VERSION=${SCALA_BINARY_VERSION} \
  --build-arg SPARK_VERSION=${SPARK_VERSION} \
  --build-arg SPARK_BINARY_VERSION=${SPARK_BINARY_VERSION} \
  --file "${SELF_DIR}/image/scc-spark.Dockerfile" \
  --tag scc-spark:${PROJECT_VERSION} \
  "${SELF_DIR}/image" $@

docker build \
  --build-arg APACHE_MIRROR=${APACHE_MIRROR} \
  --build-arg MAVEN_MIRROR=${MAVEN_MIRROR} \
  --build-arg PROJECT_VERSION=${PROJECT_VERSION} \
  --build-arg AWS_JAVA_SDK_VERSION=${AWS_JAVA_SDK_VERSION} \
  --build-arg HADOOP_VERSION=${HADOOP_VERSION} \
  --build-arg KYUUBI_VERSION=${KYUUBI_VERSION} \
  --file "${SELF_DIR}/image/scc-kyuubi.Dockerfile" \
  --tag scc-kyuubi:${PROJECT_VERSION} \
  "${SELF_DIR}/image" $@
