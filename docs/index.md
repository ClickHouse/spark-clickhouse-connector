---
hide:
  - navigation

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

Spark ClickHouse Connector is a high performance connector build on top of Spark DataSource V2.

<figure markdown>
  ![Overview](imgs/scc_overview.drawio.png)
</figure>

## Requirements

1. Basic knowledge of [Apache Spark](https://spark.apache.org/docs/latest/) and [ClickHouse](https://clickhouse.com/docs/en/).
2. An available ClickHouse single node or cluster.
3. An available Spark cluster, check the following **Compatible Matrix** to make sure the Spark version is compatible with this Connector.
4. Make sure your network policy satisfies the following requirements, both driver and executor of Spark need to access 
   ClickHouse HTTP port. If you are using it to access ClickHouse cluster, ensure the connectivity between driver and
   executor of Spark and each node of ClickHouse cluster.

## Notes

1. Integration tests based on Java 8 & 17, Scala 2.12 & 2.13, Spark {{ spark_binary_version }} and ClickHouse
   v{{ clickhouse_version }}, with both single ClickHouse instance and ClickHouse cluster.

## Compatible Matrix

For old versions, please refer the compatible matrix.

| Version | Compatible Spark Versions | ClickHouse JDBC version |
|---------|---------------------------|-------------------------|
| main    | Spark 3.3, 3.4, 3.5       | 0.6.2                   |
| 0.7.3   | Spark 3.3, 3.4            | 0.4.6                   |
| 0.6.0   | Spark 3.3                 | 0.3.2-patch11           |
| 0.5.0   | Spark 3.2, 3.3            | 0.3.2-patch11           |
| 0.4.0   | Spark 3.2, 3.3            | Not depend on           |
| 0.3.0   | Spark 3.2, 3.3            | Not depend on           |
| 0.2.1   | Spark 3.2                 | Not depend on           |
| 0.1.2   | Spark 3.2                 | Not depend on           |
