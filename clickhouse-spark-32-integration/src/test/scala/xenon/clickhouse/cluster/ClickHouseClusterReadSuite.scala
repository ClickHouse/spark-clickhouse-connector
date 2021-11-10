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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.QueryTest._
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.READ_DISTRIBUTED_CONVERT_LOCAL
import xenon.clickhouse.{BaseSparkSuite, Logging}
import xenon.clickhouse.base.ClickHouseClusterMixIn

class ClickHouseClusterReadSuite extends BaseSparkSuite
    with ClickHouseClusterMixIn
    with SparkClickHouseClusterMixin
    with SparkClickHouseClusterTestHelper
    with Logging {

  test("clickhouse metadata column - distributed table") {
    val cluster = "single_replica"
    val db = "db_w"
    val tbl_dist = "t_dist"

    withDistTable(cluster, db, tbl_dist, true) {
      assert(READ_DISTRIBUTED_CONVERT_LOCAL.defaultValueString == "true")

      // `_shard_num` is dedicated for Distributed table
      val cause = intercept[AnalysisException] {
        spark.sql(s"SELECT y, _shard_num FROM $db.$tbl_dist")
      }
      assert(cause.message.contains("cannot resolve '_shard_num' given input columns"))

      spark.sql(s"SET ${READ_DISTRIBUTED_CONVERT_LOCAL.key}=false")
      checkAnswer(
        spark.sql(s"SELECT y, _shard_num FROM $db.$tbl_dist"),
        Seq(
          Row(2021, 2),
          Row(2022, 3),
          Row(2023, 4),
          Row(2024, 1)
        )
      )

      // reset
      spark.sql(s"SET ${READ_DISTRIBUTED_CONVERT_LOCAL.key}=true")

      // infiniteLoop()
    }
  }
}
