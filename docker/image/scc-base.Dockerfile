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

FROM azul/zulu-openjdk:8
LABEL org.opencontainers.image.authors="Cheng Pan<chengpan@apache.com>"

RUN apt-get update -q && \
    apt-get install -yq retry && \
    retry --times=3 --delay=1 -- apt-get install -yq vim less net-tools wget curl && \
    retry --times=3 --delay=1 -- apt-get install -yq python-is-python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
