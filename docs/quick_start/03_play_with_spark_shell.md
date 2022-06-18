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

Play with Spark Shell
===

## Launch Spark Shell

```shell
$SPARK_HOME/bin/spark-shell \
  --conf spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog \
  --conf spark.sql.catalog.clickhouse.host=${CLICKHOUSE_HOST:-127.0.0.1} \
  --conf spark.sql.catalog.clickhouse.grpc_port=${CLICKHOUSE_GRPC_PORT:-9100} \
  --conf spark.sql.catalog.clickhouse.user=${CLICKHOUSE_USER:-default} \
  --conf spark.sql.catalog.clickhouse.password=${CLICKHOUSE_PASSWORD:-} \
  --conf spark.sql.catalog.clickhouse.database=default \
  --jars /path/clickhouse-spark-runtime-3.3_2.12-{version}.jar
```

The following argument
```
  --jars /path/clickhouse-spark-runtime-3.3_2.12-{version}.jar
```
can be replaced by
```
  --repositories https://{maven-cental-mirror or private-nexus-repo} \
  --packages com.github.housepower:clickhouse-spark-runtime-3.3_2.12:{version}
```
to avoid copying jar to your Spark client node.

## Operations

Basic operations, e.g. create database, create table, write table, read table, etc.
```
scala> spark.sql("use clickhouse")
res0: org.apache.spark.sql.DataFrame = []

scala> spark.sql("create database test_db")
res1: org.apache.spark.sql.DataFrame = []

scala> spark.sql("show databases").show
+---------+
|namespace|
+---------+
|  default|
|   system|
|  test_db|
+---------+

scala> spark.sql("""
     | CREATE TABLE test_db.tbl (
     |   create_time TIMESTAMP NOT NULL,
     |   m           INT       NOT NULL COMMENT 'part key',
     |   id          BIGINT    NOT NULL COMMENT 'sort key',
     |   value       STRING
     | ) USING ClickHouse
     | PARTITIONED BY (m)
     | TBLPROPERTIES (
     |   engine = 'MergeTree()',
     |   order_by = 'id',
     |   settings.index_granularity = 8192
     | )
     | """)
res2: org.apache.spark.sql.DataFrame = []

scala> :paste
// Entering paste mode (ctrl-D to finish)

spark.createDataFrame(Seq(
    ("2021-01-01 10:10:10", 1L, "1"),
    ("2022-02-02 10:10:10", 2L, "2")
)).toDF("create_time", "id", "value")
    .withColumn("create_time", to_timestamp($"create_time"))
    .withColumn("m", month($"create_time"))
    .select($"create_time", $"m", $"id", $"value")
    .writeTo("test_db.tbl")
    .append

// Exiting paste mode, now interpreting.

scala> spark.table("test_db.tbl").show
+-------------------+---+---+-----+
|        create_time|  m| id|value|
+-------------------+---+---+-----+
|2021-01-01 10:10:10|  1|  1|    1|
|2022-02-02 10:10:10|  2|  2|    2|
+-------------------+---+---+-----+
```
