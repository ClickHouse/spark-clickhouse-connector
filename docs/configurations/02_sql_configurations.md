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

!!! tip "Since 0.1.0 - spark.clickhouse.write.batchSize"

    Default Value: 10000

    Description: The number of records per batch on writing to ClickHouse.

!!! tip "Since 0.1.0 - spark.clickhouse.write.maxRetry"

    Default Value: 3

    Description: The maximum number of write we will retry for a single batch write failed with retryable codes.

!!! tip "Since 0.1.0 - spark.clickhouse.write.retryInterval"

    Default Value: 10

    Description: The interval in seconds between write retry.

!!! tip "Since 0.1.0 - spark.clickhouse.write.retryableErrorCodes"

    Default Value: 241

    Description: The retryable error codes returned by ClickHouse server when write failing.

!!! tip "Since 0.1.0 - spark.clickhouse.write.repartitionNum"

    Default Value: 0

    Description: Repartition data to meet the distributions of ClickHouse table is required before writing, use this
                 conf to specific the repartition number, value less than 1 mean no requirement.

!!! tip "Since 0.3.0 - spark.clickhouse.write.repartitionByPartition"

    Default Value: true

    Description: Whether to repartition data by ClickHouse partition keys to meet the distributions of ClickHouse table
                 before writing.

!!! tip "Since 0.3.0 - spark.clickhouse.write.repartitionStrictly"

    Default Value: false

    Description: If true, Spark will strictly distribute incoming records across partitions to satisfy
                 the required distribution before passing the records to the data source table on write.
                 Otherwise, Spark may apply certain optimizations to speed up the query but break the
                 distribution requirement. Note, this configuration requires SPARK-37523, w/o this patch,
                 it always act as `true`.

!!! tip "Since 0.1.0 - spark.clickhouse.write.distributed.useClusterNodes"

    Default Value: true

    Description: Write to all nodes of cluster when writing Distributed table.

!!! tip "Since 0.1.0 - spark.clickhouse.read.distributed.useClusterNodes"

    Default Value: false

    Description: Read from all nodes of cluster when reading Distributed table.

!!! tip "Since 0.1.0 - spark.clickhouse.write.distributed.convertLocal"

    Default Value: false

    Description: When writing Distributed table, write local table instead of itself. If `true`, ignore
                 `write.distributed.useClusterNodes`.

!!! tip "Since 0.1.0 - spark.clickhouse.read.distributed.convertLocal"

    Default Value: true

    Description: When reading Distributed table, read local table instead of itself. If `true`, ignore
                 `read.distributed.useClusterNodes`.

!!! tip "Since 0.4.0 - spark.clickhouse.read.splitByPartitionId"

    Default Value: true

    Description: If `true`, construct input partition filter by virtual column `_partition_id`,
                 instead of partition value. There are known bugs to assemble SQL predication by
                 partition value. This feature requires ClickHouse Server v21.6+.

!!! tip "Since 0.3.0 - spark.clickhouse.write.localSortByPartition"

    Default Value: `spark.clickhouse.write.repartitionByPartition`

    Description: If `true`, do local sort by partition before writing.

!!! tip "Since 0.3.0 - spark.clickhouse.write.localSortByKey"

    Default Value: true

    Description: If `true`, do local sort by sort keys before writing.

!!! tip "Since 0.4.0 - spark.clickhouse.ignoreUnsupportedTransform"

    Default Value: false

    Description: ClickHouse supports using complex expressions as sharding keys or partition values,
                 e.g. `cityHash64(col_1, col_2)`, and those can not be supported by Spark now. If `true`,
                 ignore the unsupported expressions, otherwise fail fast w/ an exception. Note: when
                 `spark.clickhouse.write.distributed.convertLocal` is enabled, ignore unsupported sharding keys
                 may corrupt the data.

!!! tip "Since 0.3.0 - spark.clickhouse.write.compression.codec"

    Default Value: lz4

    Description: The codec used to compress data for writing. Supported codecs: none, gzip, lz4, zstd.
                 This feature requires ClickHouse Server v22.2+, more details at
                 https://github.com/ClickHouse/ClickHouse/pull/34408

!!! tip "Since 0.4.0 - spark.clickhouse.write.compression.zstd.level"

    Default Value: 3

    Description: Zstd compression level.

!!! tip "Since 0.4.0 - spark.clickhouse.write.compression.zstd.thread"

    Default Value: 0

    Description: Zstd worker pool size. Default value is `0`, aka 'single-threaded mode': no worker is spawned.
                 Spawn worker threads and trigger asynchronous mode when value equals or greater than 1.

!!! tip "Since 0.4.0 - spark.clickhouse.write.format"

    Default Value: ArrowStream

    Description: Serialize format for writing. Supported formats: JSONEachRow, ArrowStream.
