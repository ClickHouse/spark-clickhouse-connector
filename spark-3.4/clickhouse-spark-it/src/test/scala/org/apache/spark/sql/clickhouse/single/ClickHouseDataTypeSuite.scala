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
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.USE_NULLABLE_QUERY_SCHEMA
import org.apache.spark.sql.clickhouse.SparkUtils
import org.apache.spark.sql.types.DataTypes.{createArrayType, createMapType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.tags.Cloud

import java.math.MathContext

@Cloud
class ClickHouseCloudDataTypeSuite extends ClickHouseDataTypeSuite with ClickHouseCloudMixIn

class ClickHouseSingleDataTypeSuite extends ClickHouseDataTypeSuite with ClickHouseSingleMixIn

abstract class ClickHouseDataTypeSuite extends SparkClickHouseSingleTest {

  val SPARK_43390_ENABLED: Boolean = sys.env.contains("SPARK_43390_ENABLED") || {
    SparkUtils.MAJOR_MINOR_VERSION match {
      case (major, _) if major > 3 => true
      case (3, minor) if minor > 4 => true
      case _ => false
    }
  }

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
      val respectNullable = SPARK_43390_ENABLED && !spark.conf.get(USE_NULLABLE_QUERY_SCHEMA)
      if (respectNullable) {
        assert(StructType(schema) === tblSchema)
      } else {
        val nullableFields =
          schema.fields.map(structField => structField.copy(dataType = structField.dataType.asNullable))
        assert(StructType(nullableFields) === tblSchema)
      }

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

  // "allow_experimental_bigint_types" setting is removed since v21.7.1.7020-testing
  // https://github.com/ClickHouse/ClickHouse/pull/24812
  val BIGINT_TYPES: Seq[String] = Seq("Int128", "UInt128", "Int256", "UInt256")

  // TODO - Supply more test cases
  //     1. data type alias
  //     2. negative cases
  //     3. unsupported integer types
  Seq(
    ("Int8", -128.toByte, 127.toByte),
    ("UInt8", 0.toShort, 255.toShort),
    ("Int16", -32768.toShort, 32767.toShort),
    ("UInt16", 0, 65535),
    ("Int32", -2147483648, 2147483647),
    ("UInt32", 0L, 4294967295L),
    ("Int64", -9223372036854775808L, 9223372036854775807L),
    // Only overlapping value range of both the ClickHouse type and the Spark type is supported
    ("UInt64", 0L, 4294967295L),
    ("Int128", BigDecimal("-" + "9" * 38), BigDecimal("9" * 38)),
    ("UInt128", BigDecimal(0), BigDecimal("9" * 38)),
    ("Int256", BigDecimal("-" + "9" * 38), BigDecimal("9" * 38)),
    ("UInt256", BigDecimal(0), BigDecimal("9" * 38))
  ).foreach { case (dataType, lower, upper) =>
    test(s"DateType - $dataType") {
      if (BIGINT_TYPES.contains(dataType)) {
        assume(clickhouseVersion.isNewerOrEqualTo("21.7.1.7020"))
      }
      testDataType(dataType) { (db, tbl) =>
        runClickHouseSQL(
          s"""INSERT INTO $db.$tbl VALUES
             |(1, $lower),
             |(2, $upper)
             |""".stripMargin
        )
      } { df =>
        checkAnswer(
          df,
          Row(1, lower) :: Row(2, upper) :: Nil
        )
        checkAnswer(
          df.filter("value > 1"),
          Row(2, upper) :: Nil
        )
      }
    }
  }

  test("DataType - DateTime") {
    testDataType("DateTime") { (db, tbl) =>
      runClickHouseSQL(
        s"""INSERT INTO $db.$tbl VALUES
           |(1, '2021-01-01 01:01:01'),
           |(2, '2022-02-02 02:02:02')
           |""".stripMargin
      )
    } { df =>
      checkAnswer(
        df,
        Row(1, timestamp("2021-01-01T01:01:01Z")) ::
          Row(2, timestamp("2022-02-02T02:02:02Z")) :: Nil
      )
      checkAnswer(
        df.filter("value > '2022-01-01 01:01:01'"),
        Row(2, timestamp("2022-02-02T02:02:02Z")) :: Nil
      )
    }
  }

  // Decimal(P, S): P - precision, S - scale, which have different support range in Spark and ClickHouse.
  //
  // Spark:
  //   Decimal(P, S): P: [ 1:38]; S: [0:P]
  // ClickHouse:
  //   Decimal(P, S): P: [ 1:76]; S: [0:P]
  //   Decimal32(S):  P: [ 1: 9]; S: [0:P]
  //   Decimal64(S):  P: [10:18]; S: [0:P]
  //   Decimal128(S): P: [19:38]; S: [0:P]
  //   Decimal256(S): P: [39:76]; S: [0:P]
  Seq(
    ("Decimal(38,9)", 38, 9),
    ("Decimal32(4)", 9, 4),
    ("Decimal64(4)", 18, 4),
    ("Decimal128(4)", 38, 4)
  ).foreach { case (dataType, p, s) =>
    test(s"DataType - $dataType") {
      testDataType(dataType) { (db, tbl) =>
        runClickHouseSQL(
          s"""INSERT INTO $db.$tbl VALUES
             |(1, '11.1')
             |""".stripMargin
        )
      } { df =>
        assert(df.schema.length === 2)
        assert(df.schema.fields(1).dataType === DecimalType(p, s))
        checkAnswer(
          df,
          Row(1, BigDecimal("11.1", new MathContext(p))) :: Nil
        )
      }
    }
  }

  private def testDataType(valueColDef: String)(prepare: (String, String) => Unit)(validate: DataFrame => Unit)
    : Unit = {
    val db = "test_kv_db"
    val tbl = "test_kv_tbl"
    if (!clickhouseVersion.isNewerOrEqualTo("23.3")) {
      Thread.sleep(1000)
    }
    withKVTable(db, tbl, valueColDef = valueColDef) {
      prepare(db, tbl)
      val df = spark.sql(s"SELECT key, value FROM $db.$tbl ORDER BY key")
      validate(df)
    }
  }
}
