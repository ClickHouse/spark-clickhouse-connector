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

import xenon.clickhouse.{BaseSparkSuite, Logging}
import xenon.clickhouse.base.ClickHouseClusterMixIn

class ClusterTableManagementSuite extends BaseSparkSuite
    with ClickHouseClusterMixIn
    with SparkClickHouseClusterMixin
    with SparkClickHouseClusterTestHelper
    with Logging {

  test("create or replace distribute table") {
    autoCleanupDistTable("single_replica", "db_cor", "tbl_cor_dist") { (cluster, db, tbl_dist, tbl_local) =>
      def createLocalTable(): Unit = spark.sql(
        s"""CREATE TABLE $db.$tbl_local (
           |  id Long NOT NULL
           |) USING ClickHouse
           |TBLPROPERTIES (
           |  cluster = '$cluster',
           |  engine = 'MergeTree()',
           |  order_by = 'id',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )

      def createOrReplaceLocalTable(): Unit = spark.sql(
        s"""CREATE OR REPLACE TABLE `$db`.`$tbl_local` (
           |  id Long NOT NULL
           |) USING ClickHouse
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = 'id',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )
      createLocalTable()
      createOrReplaceLocalTable()
      createOrReplaceLocalTable()
    }
  }
}
