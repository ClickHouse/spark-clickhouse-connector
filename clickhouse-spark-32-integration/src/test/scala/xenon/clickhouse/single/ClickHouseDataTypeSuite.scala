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

package xenon.clickhouse.single

import org.apache.spark.sql.types._
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes.createArrayType
import xenon.clickhouse.{BaseSparkSuite, Logging}
import xenon.clickhouse.base.ClickHouseSingleMixIn

class ClickHouseDataTypeSuite extends BaseSparkSuite
    with ClickHouseSingleMixIn
    with SparkClickHouseSingleMixin
    with SparkClickHouseSingleTestHelper
    with Logging {

  test("write supported data types") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField("col_string", StringType, false) ::
        StructField("col_array_string", createArrayType(StringType, false), false) :: Nil
    )
    val db = "t_w_s_db"
    val tbl = "t_w_s_tbl"
    withTable(db, tbl, schema) {
      val tblSchema = spark.table(s"$db.$tbl").schema
      // TODO v2 create table should respect element nullable of array field
      // assert(StructType(structFields) === tblSchema)

      val dataDF = spark.createDataFrame(Seq(
        (1L, "a", Seq("a", "b", "c")),
        (2L, "A", Seq("A", "B", "C"))
      )).toDF("id", "col_string", "col_array_string")

      spark.createDataFrame(dataDF.rdd, tblSchema)
        .writeTo(s"$db.$tbl")
        .append

      checkAnswer(
        spark.table(s"$db.$tbl").sort("id"),
        Row(1L, "a", Seq("a", "b", "c")) ::
          Row(2L, "A", Seq("A", "B", "C")) :: Nil
      )
    }
  }

  test("write unsupported data types") {}

  test("read supported data types") {}

  test("read unsupported data types") {}

  test("spark to clickhouse data type mappings") {}

  test("clickhouse to spark data type mappings") {}
}
