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

Deployment
===

## Jar

Put `clickhouse-spark-runtime-3.2_2.12-{version}.jar` into `$SPARK_HOME/jars/`, then you don't need to bundle the jar
into your Spark application, and `--jar` is not required when using `spark-shell` or `spark-sql`(again, for SQL-only
use cases, [Apache Kyuubi(Incubating)](https://github.com/apache/incubator-kyuubi) is recommended for Production).

## Configuration

Persist catalog configurations into `SPARK_HOME/conf/spark-defaults.conf`, then `--conf`s are not required when using
`spark-shell` or `spark-sql`.

```
spark.sql.catalog.ck_01=xenon.clickhouse.ClickHouseCatalog
spark.sql.catalog.ck_01.host=10.0.0.1
spark.sql.catalog.ck_01.grpc_port=9100
spark.sql.catalog.ck_01.user=app
spark.sql.catalog.ck_01.password=pwd
spark.sql.catalog.ck_01.database=default

spark.sql.catalog.ck_02=xenon.clickhouse.ClickHouseCatalog
spark.sql.catalog.ck_02.host=10.0.0.2
spark.sql.catalog.ck_02.grpc_port=9100
spark.sql.catalog.ck_02.user=app
spark.sql.catalog.ck_02.password=pwd
spark.sql.catalog.ck_02.database=default
```
