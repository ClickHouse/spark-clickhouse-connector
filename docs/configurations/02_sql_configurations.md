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

<!--begin-include-->
|Key | Default | Description | Since
|--- | ------- | ----------- | -----
spark.clickhouse.ignoreUnsupportedTransform|false|ClickHouse supports using complex expressions as sharding keys or partition values, e.g. `cityHash64(col_1, col_2)`, and those can not be supported by Spark now. If `true`, ignore the unsupported expressions, otherwise fail fast w/ an exception. Note, when `spark.clickhouse.write.distributed.convertLocal` is enabled, ignore unsupported sharding keys may corrupt the data.|0.4.0
spark.clickhouse.read.compression.codec|lz4|The codec used to decompress data for reading. Supported codecs: none, lz4.|0.5.0
spark.clickhouse.read.distributed.convertLocal|true|When reading Distributed table, read local table instead of itself. If `true`, ignore `spark.clickhouse.read.distributed.useClusterNodes`.|0.1.0
spark.clickhouse.read.format|json|Serialize format for reading. Supported formats: json, binary|0.6.0
spark.clickhouse.read.splitByPartitionId|true|If `true`, construct input partition filter by virtual column `_partition_id`, instead of partition value. There are known bugs to assemble SQL predication by partition value. This feature requires ClickHouse Server v21.6+|0.4.0
spark.clickhouse.useNullableQuerySchema|false|If `true`, mark all the fields of the query schema as nullable when executing `CREATE/REPLACE TABLE ... AS SELECT ...` and creating the table. Note, this configuration requires SPARK-43390(available in Spark 3.5), w/o this patch, it always acts as `true`.|0.8.0
spark.clickhouse.write.batchSize|10000|The number of records per batch on writing to ClickHouse.|0.1.0
spark.clickhouse.write.compression.codec|lz4|The codec used to compress data for writing. Supported codecs: none, lz4.|0.3.0
spark.clickhouse.write.distributed.convertLocal|false|When writing Distributed table, write local table instead of itself. If `true`, ignore `spark.clickhouse.write.distributed.useClusterNodes`.|0.1.0
spark.clickhouse.write.distributed.useClusterNodes|true|Write to all nodes of cluster when writing Distributed table.|0.1.0
spark.clickhouse.write.format|arrow|Serialize format for writing. Supported formats: json, arrow|0.4.0
spark.clickhouse.write.localSortByKey|true|If `true`, do local sort by sort keys before writing.|0.3.0
spark.clickhouse.write.localSortByPartition|<value of spark.clickhouse.write.repartitionByPartition>|If `true`, do local sort by partition before writing. If not set, it equals to `spark.clickhouse.write.repartitionByPartition`.|0.3.0
spark.clickhouse.write.maxRetry|3|The maximum number of write we will retry for a single batch write failed with retryable codes.|0.1.0
spark.clickhouse.write.repartitionByPartition|true|Whether to repartition data by ClickHouse partition keys to meet the distributions of ClickHouse table before writing.|0.3.0
spark.clickhouse.write.repartitionNum|0|Repartition data to meet the distributions of ClickHouse table is required before writing, use this conf to specific the repartition number, value less than 1 mean no requirement.|0.1.0
spark.clickhouse.write.repartitionStrictly|false|If `true`, Spark will strictly distribute incoming records across partitions to satisfy the required distribution before passing the records to the data source table on write. Otherwise, Spark may apply certain optimizations to speed up the query but break the distribution requirement. Note, this configuration requires SPARK-37523(available in Spark 3.4), w/o this patch, it always acts as `true`.|0.3.0
spark.clickhouse.write.retryInterval|10s|The interval in seconds between write retry.|0.1.0
spark.clickhouse.write.retryableErrorCodes|241|The retryable error codes returned by ClickHouse server when write failing.|0.1.0
<!--end-include-->
