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
import org.apache.spark.sql.Row
import org.scalatest.tags.Cloud

@Cloud
class ClickHouseCloudTableDDLSuite extends ClickHouseTableDDLSuite with ClickHouseCloudMixIn

class ClickHouseSingleTableDDLSuite extends ClickHouseTableDDLSuite with ClickHouseSingleMixIn

abstract class ClickHouseTableDDLSuite extends SparkClickHouseSingleTest {

  import testImplicits._

  test("clickhouse command runner") {
    withTable("default.abc") {
      runClickHouseSQL("CREATE TABLE default.abc(a UInt8) ENGINE=Log()")
      checkAnswer(
        spark.sql("""DESC default.abc""").select($"col_name", $"data_type").limit(1),
        Row("a", "smallint") :: Nil
      )
    }
  }
}
