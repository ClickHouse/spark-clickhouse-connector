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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes.{createArrayType, createMapType}
import org.apache.spark.sql.types._

class ClickHouseDataTypeSuite extends SparkClickHouseSingleTest {

  test("write supported data types") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField("col_string", StringType, false) ::
        StructField("col_date", DateType, false) ::
        StructField("col_array_string", createArrayType(StringType, false), false) ::
        StructField("col_map_string_string", createMapType(StringType, StringType, false), false) ::
        Nil
    )
    val db = "t_w_s_db"
    val tbl = "t_w_s_tbl"
    withTable(db, tbl, schema) {
      val tblSchema = spark.table(s"$db.$tbl").schema
      // TODO v2 create table should respect element nullable of array field
      // assert(StructType(structFields) === tblSchema)

      val dataDF = spark.createDataFrame(Seq(
        (1L, "a", date("1996-06-06"), Seq("a", "b", "c"), Map("a" -> "x")),
        (2L, "A", date("2022-04-12"), Seq("A", "B", "C"), Map("A" -> "X"))
      )).toDF("id", "col_string", "col_date", "col_array_string", "col_map_string_string")

      spark.createDataFrame(dataDF.rdd, tblSchema)
        .writeTo(s"$db.$tbl")
        .append

      checkAnswer(
        spark.table(s"$db.$tbl").sort("id"),
        Row(1L, "a", date("1996-06-06"), Seq("a", "b", "c"), Map("a" -> "x")) ::
          Row(2L, "A", date("2022-04-12"), Seq("A", "B", "C"), Map("A" -> "X")) :: Nil
      )
    }
  }

  test("write unsupported data types") {}

  test("read supported data types") {
    val db = "t_r_s_db"
    val tbl = "t_r_s_tbl"
    withKVTable(db, tbl, valueColDef = "DateTime") {
      runClickHouseSQL(
        s"""INSERT INTO $db.$tbl VALUES
           |(1, '2021-01-01 01:01:01'),
           |(2, '2022-02-02 02:02:02')
           |""".stripMargin
      )

      val data = spark.sql(s"SELECT key, value FROM $db.$tbl ORDER BY key")
      checkAnswer(
        data,
        Row(1, timestamp("2021-01-01T01:01:01Z")) ::
          Row(2, timestamp("2022-02-02T02:02:02Z")) :: Nil
      )
      checkAnswer(
        data.filter("value > '2022-01-01 01:01:01'"),
        Row(2, timestamp("2022-02-02T02:02:02Z")) :: Nil
      )
    }
  }

  test("read unsupported data types") {}

  test("spark to clickhouse data type mappings") {}

  test("clickhouse to spark data type mappings") {}
}
