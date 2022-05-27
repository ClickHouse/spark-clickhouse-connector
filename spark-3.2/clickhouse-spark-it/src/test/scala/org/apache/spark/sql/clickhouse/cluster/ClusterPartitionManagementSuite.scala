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

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.clickhouse.BaseSparkSuite
import xenon.clickhouse.Logging
import xenon.clickhouse.base.ClickHouseClusterMixIn

class ClusterPartitionManagementSuite extends BaseSparkSuite
    with ClickHouseClusterMixIn
    with SparkClickHouseClusterMixin
    with SparkClickHouseClusterTestHelper
    with Logging {

  test("distribute table partition") {
    withSimpleDistTable("single_replica", "db_part", "tbl_part", true) { (_, db, tbl_dist, _) =>
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl_dist"),
        Seq(Row("m=1"), Row("m=2"), Row("m=3"), Row("m=4"))
      )
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl_dist PARTITION(m = 2)"),
        Seq(Row("m=2"))
      )
      spark.sql(s"ALTER TABLE $db.$tbl_dist DROP PARTITION(m = 2)")
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl_dist"),
        Seq(Row("m=1"), Row("m=3"), Row("m=4"))
      )
    }
  }
}
