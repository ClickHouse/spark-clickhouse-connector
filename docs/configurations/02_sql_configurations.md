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
spark.clickhouse.client.queryTimeout|60s|The maximum time the ClickHouse client will wait for a single query or ping operation to complete on a NodeClient. Applied as a future-handle timeout on every client.query(...) and client.ping(...) call.|0.10.1
spark.clickhouse.ignoreUnsupportedTransform|true|ClickHouse supports using complex expressions as sharding keys or partition values, e.g. `cityHash64(col_1, col_2)`, and those can not be supported by Spark now. If `true`, ignore the unsupported expressions and log a warning, otherwise fail fast w/ an exception. Note, when `spark.clickhouse.write.distributed.convertLocal` is enabled, ignoring unsupported sharding keys may corrupt the data.|0.4.0
spark.clickhouse.read.compression.codec|lz4|The codec used to decompress data for reading. Supported codecs: none, lz4.|0.5.0
spark.clickhouse.read.distributed.convertLocal|true|When reading Distributed table, read local table instead of itself. If `true`, ignore `spark.clickhouse.read.distributed.useClusterNodes`. A split read then reads every shard's local table, each split on its own by `spark.clickhouse.read.taskSplitStrategy`.|0.1.0
spark.clickhouse.read.fixedStringAs|binary|Read ClickHouse FixedString type as the specified Spark data type. Supported types: binary, string|0.8.0
spark.clickhouse.read.format|json|Serialize format for reading. Supported formats: json, binary|0.6.0
spark.clickhouse.read.jsonAs|variant|[Spark 4.0+ only] Read ClickHouse JSON type as the specified Spark data type. Supported types: variant (VariantType), string|0.9.0
spark.clickhouse.read.primaryIndexSplit.maxParallelism|<undefined>|The most input partitions `primaryIndexSplit` splits a read into, whatever `spark.clickhouse.read.primaryIndexSplit.targetRowsPerTask` asks for. A Distributed table read by shard divides it equally among its shards, each getting at least one, so the total exceeds it only when there are more shards than that. A value of 1 reads the table in one input partition, a Distributed table through its engine. Not set by default: no limit.|0.10.3
spark.clickhouse.read.primaryIndexSplit.targetRowsPerTask|4000000|The rows `primaryIndexSplit` aims at per input partition: it cuts one range per that many rows of the granules the query's filters on key columns keep, three times Spark's default parallelism at the least and `spark.clickhouse.read.primaryIndexSplit.maxParallelism` at the most. It cuts fewer ranges when those rows span fewer granules, as a range holds at least one. A Distributed table read by shard is sized shard by shard, the least divided equally among the shards. An input partition holds more rows than this when the maximum is reached, when the table can't be split, or when one key value does.|0.10.3
spark.clickhouse.read.pushdown.topN|true|Whether to push down `ORDER BY ... LIMIT n` (top-N) to ClickHouse. When `true`, eligible sort orders combined with a LIMIT are translated into a ClickHouse `ORDER BY ... LIMIT n` clause and Spark performs a final merge across input partitions. When `false`, the `ORDER BY ... LIMIT n` is left for Spark to evaluate; plain `LIMIT n` queries (no `ORDER BY`) are unaffected.|0.10.1
spark.clickhouse.read.runtimeFilter.enabled|false|Enable runtime filter for reading.|0.8.0
spark.clickhouse.read.settings|<undefined>|Settings when read from ClickHouse. e.g. `final=1, max_execution_time=5`|0.9.0
spark.clickhouse.read.splitByPartitionId|true|If `true`, construct input partition filter by virtual column `_partition_id`, instead of partition value. There are known bugs to assemble SQL predication by partition value. This feature requires ClickHouse Server v21.6+|0.4.0
spark.clickhouse.read.taskSplitStrategy|partitionSplit|How a read is split into input partitions. `partitionSplit` reads one input partition per ClickHouse partition. `primaryIndexSplit` splits reads of MergeTree-family tables into ranges of the primary key holding about the same number of rows, cut from the primary index within the query's filters on key columns, as many as `spark.clickhouse.read.primaryIndexSplit.targetRowsPerTask` and `spark.clickhouse.read.primaryIndexSplit.maxParallelism` size the read to. `oneTask` reads the whole table in one input partition. `primaryIndexSplit` falls back to `oneTask` on ClickHouse before 24.2 and when the primary key does not start with an integer, date, date time, string or UUID column or expression. Aggregations pushed down without GROUP BY are always read as `oneTask`. A Distributed table is split only with `spark.clickhouse.read.distributed.convertLocal`: every shard's local table is split on its own, and a shard that can't be split is read in one input partition. Without it the table is read in one input partition through the Distributed engine.|0.10.3
spark.clickhouse.useNullableQuerySchema|false|If `true`, mark all the fields of the query schema as nullable when executing `CREATE/REPLACE TABLE ... AS SELECT ...` on creating the table. Note, this configuration requires SPARK-43390(available in Spark 3.5), w/o this patch, it always acts as `true`.|0.8.0
spark.clickhouse.write.batchSize|10000|The number of records per batch on writing to ClickHouse.|0.1.0
spark.clickhouse.write.compression.codec|lz4|The codec used to compress data for writing. Supported codecs: none, lz4.|0.3.0
spark.clickhouse.write.distributed.convertLocal|false|When writing Distributed table, write local table instead of itself. If `true`, ignore `spark.clickhouse.write.distributed.useClusterNodes`. This bypasses ClickHouse's native routing, requiring Spark to evaluate the sharding key. When using unsupported sharding expressions, set `spark.clickhouse.ignoreUnsupportedTransform` to `false` to prevent silent data distribution errors.|0.1.0
spark.clickhouse.write.distributed.convertLocal.allowUnsupportedSharding|false|Allow writing to Distributed tables with `convertLocal=true` and `ignoreUnsupportedTransform=true` when the sharding key is unsupported. This is dangerous and may cause data corruption due to incorrect sharding. Only set to `true` if you understand the risks and have verified your data distribution. By default, this combination will throw an error to prevent silent data corruption.|0.9.0
spark.clickhouse.write.distributed.useClusterNodes|true|Write to all nodes of cluster when writing Distributed table.|0.1.0
spark.clickhouse.write.format|arrow|Serialize format for writing. Supported formats: json, arrow|0.4.0
spark.clickhouse.write.localSortByKey|true|If `true`, do local sort by sort keys before writing.|0.3.0
spark.clickhouse.write.localSortByPartition|<value of spark.clickhouse.write.repartitionByPartition>|If `true`, do local sort by partition before writing. If not set, it equals to `spark.clickhouse.write.repartitionByPartition`.|0.3.0
spark.clickhouse.write.maxRetry|3|The maximum number of write we will retry for a single batch write failed with retryable codes.|0.1.0
spark.clickhouse.write.option.<name>|<undefined>|ClickHouse Java client insert option applied when writing. Replace `<name>` with a client option key, for example `spark.clickhouse.write.option.clickhouse_setting_log_comment`, `spark.clickhouse.write.option.http_header_<header>`, `spark.clickhouse.write.option.custom_<name>`, or a recognized unprefixed client option. Writer options override SQLConf entries with the same option key.|0.10.1
spark.clickhouse.write.repartitionByPartition|true|Whether to repartition data by ClickHouse partition keys to meet the distributions of ClickHouse table before writing.|0.3.0
spark.clickhouse.write.repartitionNum|0|Repartition data to meet the distributions of ClickHouse table is required before writing, use this conf to specific the repartition number, value less than 1 mean no requirement.|0.1.0
spark.clickhouse.write.repartitionStrictly|false|If `true`, Spark will strictly distribute incoming records across partitions to satisfy the required distribution before passing the records to the data source table on write. Otherwise, Spark may apply certain optimizations to speed up the query but break the distribution requirement. Note, this configuration requires SPARK-37523(available in Spark 3.4), w/o this patch, it always acts as `true`.|0.3.0
spark.clickhouse.write.retryInterval|10s|The interval in seconds between write retry.|0.1.0
spark.clickhouse.write.retryableErrorCodes|241|The retryable error codes returned by ClickHouse server when write failing.|0.1.0
<!--end-include-->
