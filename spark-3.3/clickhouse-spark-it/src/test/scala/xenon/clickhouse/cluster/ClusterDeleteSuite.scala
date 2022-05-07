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

class ClusterDeleteSuite extends BaseSparkSuite
    with ClickHouseClusterMixIn
    with SparkClickHouseClusterMixin
    with SparkClickHouseClusterTestHelper
    with Logging {

  test("truncate distribute table") {
    withSimpleDistTable("single_replica", "db_truncate", "tbl_truncate", true) { (_, db, tbl_dist, _) =>
      assert(spark.table(s"$db.$tbl_dist").count() === 4)
      spark.sql(s"TRUNCATE TABLE $db.$tbl_dist")
      assert(spark.table(s"$db.$tbl_dist").count() === 0)
    }
  }

  test("delete from distribute table") {
    withSimpleDistTable("single_replica", "db_delete", "tbl_delete", true) { (_, db, tbl_dist, _) =>
      assert(spark.table(s"$db.$tbl_dist").count() === 4)
      spark.sql(s"DELETE FROM $db.$tbl_dist WHERE m = 1")
      assert(spark.table(s"$db.$tbl_dist").count() === 3)
    }
  }
}
