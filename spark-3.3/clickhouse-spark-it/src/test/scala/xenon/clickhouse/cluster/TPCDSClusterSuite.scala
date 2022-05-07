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

package xenon.clickhouse.cluster

import org.scalatest.tags.Slow
import xenon.clickhouse.base.ClickHouseClusterMixIn
import xenon.clickhouse.{BaseSparkSuite, Logging, TPCDSHelper}

@Slow
class TPCDSClusterSuite extends BaseSparkSuite
    with ClickHouseClusterMixIn
    with SparkClickHouseClusterMixin
    with SparkClickHouseClusterTestHelper
    with TPCDSHelper
    with Logging {

  override def sparkOptions: Map[String, String] = super.sparkOptions + (
    "spark.sql.catalog.tpcds" -> "org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog",
    "spark.clickhouse.write.batchSize" -> "100000",
    "spark.clickhouse.write.distributed.convertLocal" -> "true"
  )

  test("Cluster: TPC-DS sf1 write and count(*)") {
    spark.sql("CREATE DATABASE tpcds_sf1_cluster WITH DBPROPERTIES (cluster = 'single_replica');")

    tablePrimaryKeys.foreach { case (table, primaryKeys) =>
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
    }

    tablePrimaryKeys.keys.foreach { table =>
      assert(spark.table(s"tpcds.sf1.$table").count === spark.table(s"tpcds_sf1_cluster.$table").count)
    }
  }
}
