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

Configurations
===

## TODO

## Overwrite SQL Configurations

Your can overwrite [ClickHouse SQL Configurations](./02_sql_configurations.md) by editing
`$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.clickhouse.write.batchSize          10000
spark.clickhouse.write.maxRetry           2
```