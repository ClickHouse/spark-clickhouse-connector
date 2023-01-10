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
### Single Instance

Suppose you have one ClickHouse instance which installed on `10.0.0.1` and exposes HTTP on `8123`.

Edit `$SPARK_HOME/conf/spark-defaults.conf`.

```
# register a catalog named "clickhouse"
spark.sql.catalog.clickhouse                      xenon.clickhouse.ClickHouseCatalog

# basic configurations for "clickhouse" catalog
spark.sql.catalog.clickhouse.host                 10.0.0.1
spark.sql.catalog.clickhouse.protocol             http
spark.sql.catalog.clickhouse.http_port            8123
spark.sql.catalog.clickhouse.user                 default
spark.sql.catalog.clickhouse.password
spark.sql.catalog.clickhouse.database             default

# custom options of clickhouse-client for "clickhouse" catalog
spark.sql.catalog.clickhouse.option.async         false
spark.sql.catalog.clickhouse.option.client_name   spark
```

Then you can access ClickHouse table `<ck_db>.<ck_table>` from Spark SQL by using `clickhouse.<ck_db>.<ck_table>`.

### Cluster

For ClickHouse cluster, give an unique catalog name for each instances.

Suppose you have two ClickHouse instances, one installed on `10.0.0.1` and exposes gRPC on port `9100` named
clickhouse1, and another installed on `10.0.0.2` and exposes gRPC on port `9100` named clickhouse2.

Edit `$SPARK_HOME/conf/spark-defaults.conf`.

```
spark.sql.catalog.clickhouse1                xenon.clickhouse.ClickHouseCatalog
spark.sql.catalog.clickhouse1.host           10.0.0.1
spark.sql.catalog.clickhouse1.protocol       grpc
spark.sql.catalog.clickhouse1.grpc_port      9100
spark.sql.catalog.clickhouse1.user           default
spark.sql.catalog.clickhouse1.password
spark.sql.catalog.clickhouse1.database       default
spark.sql.catalog.clickhouse1.option.async   false

spark.sql.catalog.clickhouse2                xenon.clickhouse.ClickHouseCatalog
spark.sql.catalog.clickhouse2.host           10.0.0.2
spark.sql.catalog.clickhouse2.protocol       grpc
spark.sql.catalog.clickhouse2.grpc_port      9100
spark.sql.catalog.clickhouse2.user           default
spark.sql.catalog.clickhouse2.password
spark.sql.catalog.clickhouse2.database       default
spark.sql.catalog.clickhouse2.option.async   false
```

Then you can access clickhouse1 table `<ck_db>.<ck_table>` from Spark SQL by `clickhouse1.<ck_db>.<ck_table>`,
and access clickhouse2 table `<ck_db>.<ck_table>` by `clickhouse2.<ck_db>.<ck_table>`.
<!--end-include-->
