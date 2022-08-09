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

How reading of the connector works?
===

## Push Down

Spark supports push down the processing of queries, or parts of queries, into the connected data source. This means that
a specific predicate, aggregation function, or other operation, could be passed through to ClickHouse for processing.

The results of this push down can include the following benefits:

- Improved overall query performance

- Reduced network traffic between Spark and ClickHouse

- Reduced load on ClickHouse

These benefits often result in significant cost reduction.

The connector implements most push down interfaces defined by DataSource V2, such as `SupportsPushDownLimit`,
`SupportsPushDownFilters`, `SupportsPushDownAggregates`, `SupportsPushDownRequiredColumns`.

The below example shows how `SupportsPushDownAggregates` and `SupportsPushDownRequiredColumns` work. 

<figure markdown>
  ![Overview](../imgs/scc_read_pushdown_disable.drawio.png)
  <figcaption>Push Down disabled</figcaption>
</figure>

<figure markdown>
  ![Overview](../imgs/scc_read_pushdown_enable.drawio.png)
  <figcaption>Push Down enabled</figcaption>
</figure>

## Bucket Join

Sort merge join is a general solution for two large table inner join, it requires two table shuffle by join key first,
then do local sort by join key in each data partition, finally do stream-stream like look up to get the final result.

In some cases, the tables store collocated by join keys, w/ 
[Storage-Partitioned Join](https://issues.apache.org/jira/browse/SPARK-37375)(or V2 Bucket Join), Spark could leverage
the existing ClickHouse table layout to eliminate the expensive shuffle and sort operations.

<figure markdown>
  ![Overview](../imgs/scc_read_sort_merge_join.drawio.png)
  <figcaption>Sort Merge Join</figcaption>
</figure>

<figure markdown>
  ![Overview](../imgs/scc_read_bucket_join.drawio.png)
  <figcaption>Bucket Join</figcaption>
</figure>
