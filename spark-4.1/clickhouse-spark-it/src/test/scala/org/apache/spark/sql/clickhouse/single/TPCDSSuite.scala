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

package org.apache.spark.sql.clickhouse.single

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn}
import org.apache.spark.SparkConf
import org.apache.spark.sql.clickhouse.TPCDSTestUtils
import org.scalatest.tags.{Cloud, Slow}

@Cloud
class ClickHouseCloudTPCDSSuite extends TPCDSSuite with ClickHouseCloudMixIn

@Slow
class ClickHouseSingleTPCDSSuite extends TPCDSSuite with ClickHouseSingleMixIn

abstract class TPCDSSuite extends SparkClickHouseSingleTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.tpcds", "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog")
    .set("spark.sql.catalog.clickhouse.protocol", "http")
    .set("spark.clickhouse.read.compression.codec", "none")
    .set("spark.clickhouse.write.batchSize", "100000")
    .set("spark.clickhouse.write.compression.codec", "none")

  test("TPC-DS tiny write and count(*)") {
    val dbName = if (useSuiteLevelDatabase) testDatabaseName else "tpcds_tiny"
    withDatabase(dbName) {
      if (!useSuiteLevelDatabase) {
        spark.sql(s"CREATE DATABASE $dbName")
      }

      TPCDSTestUtils.tablePrimaryKeys.foreach { case (table, primaryKeys) =>
        spark.sql(
          s"""
             |CREATE TABLE $dbName.$table
             |USING clickhouse
             |TBLPROPERTIES (
             |    order_by = '${primaryKeys.mkString(",")}',
             |    'settings.allow_nullable_key' = 1
             |)
             |SELECT * FROM tpcds.tiny.$table;
             |""".stripMargin
        )
      }

      TPCDSTestUtils.tablePrimaryKeys.keys.foreach { table =>
        assert(spark.table(s"$dbName.$table").count === spark.table(s"tpcds.tiny.$table").count)
      }
    }
  }
}
