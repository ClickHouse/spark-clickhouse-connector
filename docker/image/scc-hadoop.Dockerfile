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
ARG HADOOP_VERSION

ARG APACHE_MIRROR
ARG MAVEN_MIRROR

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop/conf

RUN if [ $(uname -m) = "aarch64" ]; then HADOOP_TAR_NAME=hadoop-${HADOOP_VERSION}-aarch64; else HADOOP_TAR_NAME=hadoop-${HADOOP_VERSION}; fi && \
    wget -q ${APACHE_MIRROR}/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_TAR_NAME}.tar.gz && \
    tar -xzf ${HADOOP_TAR_NAME}.tar.gz -C /opt && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm ${HADOOP_TAR_NAME}.tar.gz && \
    HADOOP_CLOUD_STORAGE_JAR_NAME=hadoop-cloud-storage && \
    wget -q ${MAVEN_MIRROR}/org/apache/hadoop/${HADOOP_CLOUD_STORAGE_JAR_NAME}/${HADOOP_VERSION}/${HADOOP_CLOUD_STORAGE_JAR_NAME}-${HADOOP_VERSION}.jar -P ${HADOOP_HOME}/share/hadoop/hdfs/lib && \
    HADOOP_AWS_JAR_NAME=hadoop-aws && \
    wget -q ${MAVEN_MIRROR}/org/apache/hadoop/${HADOOP_AWS_JAR_NAME}/${HADOOP_VERSION}/${HADOOP_AWS_JAR_NAME}-${HADOOP_VERSION}.jar -P ${HADOOP_HOME}/share/hadoop/hdfs/lib && \
    AWS_JAVA_SDK_BUNDLE_JAR_NAME=aws-java-sdk-bundle && \
    wget -q ${MAVEN_MIRROR}/com/amazonaws/${AWS_JAVA_SDK_BUNDLE_JAR_NAME}/${AWS_JAVA_SDK_VERSION}/${AWS_JAVA_SDK_BUNDLE_JAR_NAME}-${AWS_JAVA_SDK_VERSION}.jar -P ${HADOOP_HOME}/share/hadoop/hdfs/lib
