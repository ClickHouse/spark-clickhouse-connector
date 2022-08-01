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

Play with Spark SQL
===

Note: For SQL-only use cases, [Apache Kyuubi(Incubating)](https://github.com/apache/incubator-kyuubi) is recommended
for Production.

## Launch Spark SQL CLI

```shell
$SPARK_HOME/bin/spark-sql \
  --conf spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog \
  --conf spark.sql.catalog.clickhouse.host=${CLICKHOUSE_HOST:-127.0.0.1} \
  --conf spark.sql.catalog.clickhouse.grpc_port=${CLICKHOUSE_GRPC_PORT:-9100} \
  --conf spark.sql.catalog.clickhouse.user=${CLICKHOUSE_USER:-default} \
  --conf spark.sql.catalog.clickhouse.password=${CLICKHOUSE_PASSWORD:-} \
  --conf spark.sql.catalog.clickhouse.database=default \
  --jars /path/clickhouse-spark-runtime-3.3_2.12-{version}.jar \
  --jars /path/clickhouse-jdbc-0.3.2-patch11-all.jar
```

The following argument
```
  --jars /path/clickhouse-spark-runtime-3.3_2.12-{version}.jar \
  --jars /path/clickhouse-jdbc-0.3.2-patch11-all.jar
```
can be replaced by
```
  --repositories https://{maven-cental-mirror or private-nexus-repo} \
  --packages com.github.housepower:clickhouse-spark-runtime-3.3_2.12:{version} \
  --packages com.clickhouse:clickhouse-jdbc:0.3.2-patch11:all
```
to avoid copying jar to your Spark client node.

## Operations

Basic operations, e.g. create database, create table, write table, read table, etc.
```
spark-sql> use clickhouse;
Time taken: 0.016 seconds

spark-sql> create database if not exists test_db;
Time taken: 0.022 seconds

spark-sql> show databases;
default
system
test_db
Time taken: 0.289 seconds, Fetched 3 row(s)

spark-sql> CREATE TABLE test_db.tbl_sql (
         >   create_time TIMESTAMP NOT NULL,
         >   m           INT       NOT NULL COMMENT 'part key',
         >   id          BIGINT    NOT NULL COMMENT 'sort key',
         >   value       STRING
         > ) USING ClickHouse
         > PARTITIONED BY (m)
         > TBLPROPERTIES (
         >   engine = 'MergeTree()',
         >   order_by = 'id',
         >   settings.index_granularity = 8192
         > );
Time taken: 0.242 seconds

spark-sql> insert into test_db.tbl_sql values
         > (timestamp'2021-01-01 10:10:10', 1, 1L, '1'),
         > (timestamp'2022-02-02 10:10:10', 2, 2L, '2')
         > as tabl(create_time, m, id, value);
Time taken: 0.276 seconds

spark-sql> select * from test_db.tbl_sql;
2021-01-01 10:10:10	1	1	1
2022-02-02 10:10:10	2	2	2
Time taken: 0.116 seconds, Fetched 2 row(s)

spark-sql> insert into test_db.tbl_sql select * from test_db.tbl_sql;
Time taken: 1.028 seconds

spark-sql> insert into test_db.tbl_sql select * from test_db.tbl_sql;
Time taken: 0.462 seconds

spark-sql> select count(*) from test_db.tbl_sql;
6
Time taken: 1.421 seconds, Fetched 1 row(s)

spark-sql> select * from test_db.tbl_sql;
2021-01-01 10:10:10	1	1	1
2021-01-01 10:10:10	1	1	1
2021-01-01 10:10:10	1	1	1
2022-02-02 10:10:10	2	2	2
2022-02-02 10:10:10	2	2	2
2022-02-02 10:10:10	2	2	2
Time taken: 0.123 seconds, Fetched 6 row(s)
```
