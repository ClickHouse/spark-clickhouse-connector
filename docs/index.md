---
license: |
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      https://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  
  See the License for the specific language governing permissions and
  limitations under the License.
---

Overview
===

Spark ClickHouse Connector is a high performance connector build on top of Spark DataSource V2 and
ClickHouse gRPC protocol.

## Requirement

1. Basic knowledge of [Apache Spark](https://spark.apache.org/docs/latest/).
2. Basic knowledge of [ClickHouse](https://clickhouse.com/docs/en/).
3. An available ClickHouse single node or cluster, and ClickHouse version should at least [v21.1.2.15-stable](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md#clickhouse-release-v211215-stable-2021-01-18),
   because Spark communicate with ClickHouse through the gRPC protocol.
4. An available Spark cluster, and Spark version should be 3.2.x, because we need the interfaces of Spark DataSource V2
   added in 3.2.0.
5. Check your network strategy, both driver and executor of Spark need to communicate with ClickHouse nodes.

## Notes

1. Integration tests based on Spark v3.2.0 and ClickHouse v21.8.10.19-lts, cover both single mode and cluster mode.
