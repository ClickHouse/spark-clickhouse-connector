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

ARG PROJECT_VERSION

FROM scc-base:${PROJECT_VERSION}

ARG AWS_JAVA_SDK_VERSION
ARG DELTA_VERSION
ARG HADOOP_VERSION
ARG ICEBERG_VERSION
ARG SCALA_BINARY_VERSION
ARG SPARK_VERSION
ARG SPARK_BINARY_VERSION

ARG APACHE_MIRROR
ARG MAVEN_MIRROR

ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV HIVE_CONF_DIR=/etc/hive/conf
ENV SPARK_CONF_DIR=/etc/spark/conf

RUN wget -q ${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3.2 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.2.tgz && \
    ICEBERG_SPARK_JAR_NAME=iceberg-spark-runtime-${SPARK_BINARY_VERSION}_${SCALA_BINARY_VERSION} && \
    wget -q ${MAVEN_MIRROR}/org/apache/iceberg/${ICEBERG_SPARK_JAR_NAME}/${ICEBERG_VERSION}/${ICEBERG_SPARK_JAR_NAME}-${ICEBERG_VERSION}.jar -P ${SPARK_HOME}/jars && \
    SPARK_HADOOP_CLOUD_JAR_NAME=spark-hadoop-cloud_${SCALA_BINARY_VERSION} && \
    wget -q ${MAVEN_MIRROR}/org/apache/spark/${SPARK_HADOOP_CLOUD_JAR_NAME}/${SPARK_VERSION}/${SPARK_HADOOP_CLOUD_JAR_NAME}-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars && \
    HADOOP_CLOUD_STORAGE_JAR_NAME=hadoop-cloud-storage && \
    wget -q ${MAVEN_MIRROR}/org/apache/hadoop/${HADOOP_CLOUD_STORAGE_JAR_NAME}/${HADOOP_VERSION}/${HADOOP_CLOUD_STORAGE_JAR_NAME}-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars && \
    HADOOP_AWS_JAR_NAME=hadoop-aws && \
    wget -q ${MAVEN_MIRROR}/org/apache/hadoop/${HADOOP_AWS_JAR_NAME}/${HADOOP_VERSION}/${HADOOP_AWS_JAR_NAME}-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars && \
    AWS_JAVA_SDK_BUNDLE_JAR_NAME=aws-java-sdk-bundle && \
    wget -q ${MAVEN_MIRROR}/com/amazonaws/${AWS_JAVA_SDK_BUNDLE_JAR_NAME}/${AWS_JAVA_SDK_VERSION}/${AWS_JAVA_SDK_BUNDLE_JAR_NAME}-${AWS_JAVA_SDK_VERSION}.jar -P ${SPARK_HOME}/jars
