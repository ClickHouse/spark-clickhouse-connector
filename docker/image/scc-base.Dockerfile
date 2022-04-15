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

ARG BASE_IMAGE

FROM ${BASE_IMAGE}:20.04
LABEL org.opencontainers.image.authors="Cheng Pan<chengpan@apache.com>"

ARG UBUNTU_MIRROR
ARG JDK_ARCH

ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-${JDK_ARCH}

RUN sed -i "s/archive.ubuntu.com/${UBUNTU_MIRROR}/g" /etc/apt/sources.list && \
    sed -i "s/security.ubuntu.com/${UBUNTU_MIRROR}/g" /etc/apt/sources.list && \
    apt-get update -q && \
    apt-get install -yq retry &&  \
    retry --times=3 --delay=1 -- apt-get install -yq vim wget axel curl && \
    retry --times=3 --delay=1 -- apt-get install -yq python-is-python3 python3-pip && \
    retry --times=3 --delay=1 -- apt-get install -yq openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*
