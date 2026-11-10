/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.clickhouse.cluster

import org.apache.spark.SparkConf
import org.apache.spark.sql.clickhouse.TPCDSTestUtils
import org.scalatest.tags.Slow

@Slow
class TPCDSClusterSuite extends SparkClickHouseClusterTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.tpcds", "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog")
    .set("spark.sql.catalog.clickhouse_s1r1.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s1r2.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s2r1.protocol", "http")
    .set("spark.sql.catalog.clickhouse_s2r2.protocol", "http")
    .set("spark.clickhouse.read.compression.codec", "lz4")
    .set("spark.clickhouse.write.batchSize", "100000")
    .set("spark.clickhouse.write.compression.codec", "lz4")
    .set("spark.clickhouse.write.distributed.convertLocal", "true")
    .set("spark.clickhouse.write.format", "json")

  test("Cluster: TPC-DS sf1 write and count(*)") {
    withDatabase("tpcds_sf1_cluster") {
      spark.sql("CREATE DATABASE tpcds_sf1_cluster WITH DBPROPERTIES (cluster = 'single_replica')")

      TPCDSTestUtils.tablePrimaryKeys.foreach { case (table, primaryKeys) =>
        println(s"before table ${table} ${primaryKeys}")
        val start: Long = System.currentTimeMillis()
        spark.sql(
          s"""
             |CREATE TABLE tpcds_sf1_cluster.$table
             |USING clickhouse
             |TBLPROPERTIES (
             |    cluster = 'single_replica',
             |    engine = 'distributed',
             |    'local.order_by' = '${primaryKeys.mkString(",")}',
             |    'local.settings.allow_nullable_key' = 1
             |)
             |SELECT * FROM tpcds.sf1.$table;
             |""".stripMargin
        )
        println(s"time took  table ${table} ${System.currentTimeMillis() - start}")
      }

      TPCDSTestUtils.tablePrimaryKeys.keys.foreach { table =>
        println(s"table ${table}")
        assert(spark.table(s"tpcds.sf1.$table").count === spark.table(s"tpcds_sf1_cluster.$table").count)
      }
    }
  }
}
