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

SQL Configurations
===

!!! tip "Since 1.0.0 - spark.clickhouse.write.batchSize"

    Default Value: 10000

    Description: The number of records per batch on writing to ClickHouse.

!!! tip "Since 1.0.0 - spark.clickhouse.write.maxRetry"

    Default Value: 3

    Description: The maximum number of write we will retry for a single batch write failed with retryable codes.

!!! tip "Since 1.0.0 - spark.clickhouse.write.retryInterval"

    Default Value: 10

    Description: The interval in seconds between write retry.

!!! tip "Since 1.0.0 - spark.clickhouse.write.retryableErrorCodes"

    Default Value: 241

    Description: The retryable error codes returned by ClickHouse server when write failing.

!!! tip "Since 1.0.0 - spark.clickhouse.write.repartitionNum"

    Default Value: 0

    Description: Repartition data to meet the distributions of ClickHouse table is required before writing, use this
                 conf to specific the repartition number, value less than 1 mean no requirement.

!!! tip "Since 1.0.0 - spark.clickhouse.write.distributed.useClusterNodes"

    Default Value: true

    Description: Write to all nodes of cluster when writing Distributed table.

!!! tip "Since 1.0.0 - spark.clickhouse.read.distributed.useClusterNodes"

    Default Value: false

    Description: Read from all nodes of cluster when reading Distributed table.

!!! tip "Since 1.0.0 - spark.clickhouse.write.distributed.convertLocal"

    Default Value: false

    Description: When writing Distributed table, write local table instead of itself. If `true`, ignore
                 `write.distributed.useClusterNodes`.

!!! tip "Since 1.0.0 - spark.clickhouse.read.distributed.convertLocal"

    Default Value: true

    Description: When reading Distributed table, read local table instead of itself. If `true`, ignore
                 `read.distributed.useClusterNodes`.

!!! tip "Since 1.0.0 - spark.clickhouse.truncate.distributed.convertLocal"

    Default Value: true

    Description: When truncate Distributed table, truncate local table instead of itself.
