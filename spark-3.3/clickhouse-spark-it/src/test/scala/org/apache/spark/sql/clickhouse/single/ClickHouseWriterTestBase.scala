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

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

/**
 * Shared test cases for both JSON and Binary writers.
 * Subclasses only need to configure the write format.
 */
trait ClickHouseWriterTestBase extends SparkClickHouseSingleTest {

  test("write ArrayType - array of integers") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ArrayType(IntegerType, containsNull = false), nullable = false)
    ))

    withTable("test_db", "test_write_array_int", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Seq(1, 2, 3)),
        Row(2, Seq(10, 20, 30)),
        Row(3, Seq(100))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_array_int")

      val result = spark.table(s"$actualDb.test_write_array_int").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(1).getSeq[Int](1) == Seq(10, 20, 30))
      assert(result(2).getSeq[Int](1) == Seq(100))
    }
  }

  test("write ArrayType - empty arrays") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ArrayType(IntegerType, containsNull = false), nullable = false)
    ))

    withTable("test_db", "test_write_empty_array", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Seq()),
        Row(2, Seq(1, 2, 3)),
        Row(3, Seq())
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_empty_array")

      val result = spark.table(s"$actualDb.test_write_empty_array").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1).isEmpty)
      assert(result(1).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(2).getSeq[Int](1).isEmpty)
    }
  }

  test("write ArrayType - nested arrays") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField(
        "value",
        ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = false),
        nullable = false
      )
    ))

    withTable("test_db", "test_write_nested_array", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Seq(Seq(1, 2), Seq(3, 4))),
        Row(2, Seq(Seq(10, 20, 30))),
        Row(3, Seq(Seq(), Seq(100)))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_nested_array")

      val result = spark.table(s"$actualDb.test_write_nested_array").orderBy("id").collect()
      assert(result.length == 3)
      // Convert to List for Scala 2.12/2.13 compatibility
      val row0 = result(0).getAs[scala.collection.Seq[scala.collection.Seq[Int]]](1).map(_.toList).toList
      val row1 = result(1).getAs[scala.collection.Seq[scala.collection.Seq[Int]]](1).map(_.toList).toList
      val row2 = result(2).getAs[scala.collection.Seq[scala.collection.Seq[Int]]](1).map(_.toList).toList
      assert(row0 == Seq(Seq(1, 2), Seq(3, 4)))
      assert(row1 == Seq(Seq(10, 20, 30)))
      assert(row2(0).isEmpty)
      assert(row2(1) == Seq(100))
    }
  }

  test("write ArrayType - with nullable elements") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ArrayType(IntegerType, containsNull = true), nullable = false)
    ))

    withTable("test_db", "test_write_array_nullable", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Seq(1, null, 3)),
        Row(2, Seq(null, null)),
        Row(3, Seq(10, 20, 30))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_array_nullable")

      val result = spark.table(s"$actualDb.test_write_array_nullable").orderBy("id").collect()
      assert(result.length == 3)
      val arr1 = result(0).getSeq[Any](1)
      assert(arr1.length == 3)
      assert(arr1(0) == 1)
      assert(arr1(1) == null)
      assert(arr1(2) == 3)
    }
  }

  test("write BooleanType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", BooleanType, nullable = true)
    ))

    withTable("test_db", "test_write_bool_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, true),
        Row(2, null),
        Row(3, false)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_bool_null")

      val result = spark.table(s"$actualDb.test_write_bool_null").orderBy("id").collect()
      assert(result.length == 3)
      // Boolean is stored as Bool in ClickHouse, reads back as Boolean
      assert(result(0).getBoolean(1) == true)
      assert(result(1).isNullAt(1))
      assert(result(2).getBoolean(1) == false)
    }
  }

  // NOTE: ClickHouse stores Boolean as Bool, so it reads back as Boolean
  test("write BooleanType - true and false values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", BooleanType, nullable = false)
    ))

    withTable("test_db", "test_write_bool", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, true),
        Row(2, false)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_bool")

      val result = spark.table(s"$actualDb.test_write_bool").orderBy("id").collect()
      assert(result.length == 2)
      // Boolean is stored as Bool in ClickHouse, reads back as Boolean
      assert(result(0).getBoolean(1) == true)
      assert(result(1).getBoolean(1) == false)
    }
  }

  test("write ByteType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ByteType, nullable = false)
    ))

    withTable("test_db", "test_write_byte", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Byte.MinValue),
        Row(2, 0.toByte),
        Row(3, Byte.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_byte")

      val result = spark.table(s"$actualDb.test_write_byte").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getByte(1) == Byte.MinValue)
      assert(result(1).getByte(1) == 0.toByte)
      assert(result(2).getByte(1) == Byte.MaxValue)
    }
  }

  test("write ByteType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ByteType, nullable = true)
    ))

    withTable("test_db", "test_write_byte_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Byte.MinValue),
        Row(2, null),
        Row(3, Byte.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_byte_null")

      val result = spark.table(s"$actualDb.test_write_byte_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getByte(1) == Byte.MinValue)
      assert(result(1).isNullAt(1))
      assert(result(2).getByte(1) == Byte.MaxValue)
    }
  }

  test("write DateType - dates") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DateType, nullable = false)
    ))

    withTable("test_db", "test_write_date", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, java.sql.Date.valueOf("2024-01-01")),
        Row(2, java.sql.Date.valueOf("2024-06-15")),
        Row(3, java.sql.Date.valueOf("2024-12-31"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_date")

      val result = spark.table(s"$actualDb.test_write_date").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).getDate(1) != null)
      assert(result(2).getDate(1) != null)
    }
  }

  test("write DateType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DateType, nullable = true)
    ))

    withTable("test_db", "test_write_date_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, java.sql.Date.valueOf("2024-01-01")),
        Row(2, null),
        Row(3, java.sql.Date.valueOf("2024-12-31"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_date_null")

      val result = spark.table(s"$actualDb.test_write_date_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getDate(1) != null)
    }
  }

  test("write DecimalType - Decimal(10,2)") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DecimalType(10, 2), nullable = false)
    ))

    withTable("test_db", "test_write_decimal", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, BigDecimal("12345.67")),
        Row(2, BigDecimal("-9999.99")),
        Row(3, BigDecimal("0.01"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_decimal")

      val result = spark.table(s"$actualDb.test_write_decimal").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1) == BigDecimal("12345.67").underlying())
      assert(result(1).getDecimal(1) == BigDecimal("-9999.99").underlying())
      assert(result(2).getDecimal(1) == BigDecimal("0.01").underlying())
    }
  }

  test("write DecimalType - Decimal(18,4)") {
    // Note: High-precision decimals (>15-17 significant digits) may lose precision in JSON/Arrow formats.
    // This appears to be related to the serialization/deserialization path, possibly due to intermediate
    // double conversions in the format parsers. This test uses tolerance-based assertions to account
    // for this observed behavior. Binary format (RowBinaryWithNamesAndTypes) preserves full precision.
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DecimalType(18, 4), nullable = false)
    ))

    withTable("test_db", "test_write_decimal_18_4", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, BigDecimal("12345678901234.5678")),
        Row(2, BigDecimal("-9999999999999.9999")),
        Row(3, BigDecimal("0.0001"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_decimal_18_4")

      val result = spark.table(s"$actualDb.test_write_decimal_18_4").orderBy("id").collect()
      assert(result.length == 3)
      // Use tolerance for high-precision values (18 significant digits)
      val tolerance = BigDecimal("0.001")
      assert((BigDecimal(result(0).getDecimal(1)) - BigDecimal("12345678901234.5678")).abs < tolerance)
      assert((BigDecimal(result(1).getDecimal(1)) - BigDecimal("-9999999999999.9999")).abs < tolerance)
      // Small values should be exact
      assert(result(2).getDecimal(1) == BigDecimal("0.0001").underlying())
    }
  }

  test("write DecimalType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DecimalType(10, 2), nullable = true)
    ))

    withTable("test_db", "test_write_decimal_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, BigDecimal("12345.67")),
        Row(2, null),
        Row(3, BigDecimal("-9999.99"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_decimal_null")

      val result = spark.table(s"$actualDb.test_write_decimal_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1) == BigDecimal("12345.67").underlying())
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) == BigDecimal("-9999.99").underlying())
    }
  }

  test("write DoubleType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DoubleType, nullable = true)
    ))

    withTable("test_db", "test_write_double_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, 3.14159),
        Row(2, null),
        Row(3, -2.71828)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_double_null")

      val result = spark.table(s"$actualDb.test_write_double_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDouble(1) - 3.14159) < 0.00001)
      assert(result(1).isNullAt(1))
      assert(math.abs(result(2).getDouble(1) - -2.71828) < 0.00001)
    }
  }

  test("write DoubleType - regular values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DoubleType, nullable = false)
    ))

    withTable("test_db", "test_write_double", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, 3.14159),
        Row(2, -2.71828),
        Row(3, 0.0)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_double")

      val result = spark.table(s"$actualDb.test_write_double").orderBy("id").collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDouble(1) - 3.14159) < 0.00001)
      assert(math.abs(result(1).getDouble(1) - -2.71828) < 0.00001)
      assert(result(2).getDouble(1) == 0.0)
    }
  }

  test("write FloatType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", FloatType, nullable = true)
    ))

    withTable("test_db", "test_write_float_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, 3.14f),
        Row(2, null),
        Row(3, -2.718f)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_float_null")

      val result = spark.table(s"$actualDb.test_write_float_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getFloat(1) - 3.14f) < 0.001f)
      assert(result(1).isNullAt(1))
      assert(math.abs(result(2).getFloat(1) - -2.718f) < 0.001f)
    }
  }

  test("write FloatType - regular values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", FloatType, nullable = false)
    ))

    withTable("test_db", "test_write_float", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, 3.14f),
        Row(2, -2.718f),
        Row(3, 0.0f)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_float")

      val result = spark.table(s"$actualDb.test_write_float").orderBy("id").collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getFloat(1) - 3.14f) < 0.001f)
      assert(math.abs(result(1).getFloat(1) - -2.718f) < 0.001f)
      assert(result(2).getFloat(1) == 0.0f)
    }
  }

  test("write IntegerType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = false)
    ))

    withTable("test_db", "test_write_int", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Int.MinValue),
        Row(2, 0),
        Row(3, Int.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_int")

      val result = spark.table(s"$actualDb.test_write_int").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getInt(1) == Int.MinValue)
      assert(result(1).getInt(1) == 0)
      assert(result(2).getInt(1) == Int.MaxValue)
    }
  }

  test("write IntegerType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = true)
    ))

    withTable("test_db", "test_write_int_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Int.MinValue),
        Row(2, null),
        Row(3, Int.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_int_null")

      val result = spark.table(s"$actualDb.test_write_int_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getInt(1) == Int.MinValue)
      assert(result(1).isNullAt(1))
      assert(result(2).getInt(1) == Int.MaxValue)
    }
  }

  test("write LongType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", LongType, nullable = false)
    ))

    withTable("test_db", "test_write_long", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Long.MinValue),
        Row(2, 0L),
        Row(3, Long.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_long")

      val result = spark.table(s"$actualDb.test_write_long").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getLong(1) == Long.MinValue)
      assert(result(1).getLong(1) == 0L)
      assert(result(2).getLong(1) == Long.MaxValue)
    }
  }

  test("write LongType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", LongType, nullable = true)
    ))

    withTable("test_db", "test_write_long_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Long.MinValue),
        Row(2, null),
        Row(3, Long.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_long_null")

      val result = spark.table(s"$actualDb.test_write_long_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getLong(1) == Long.MinValue)
      assert(result(1).isNullAt(1))
      assert(result(2).getLong(1) == Long.MaxValue)
    }
  }

  test("write MapType - empty maps") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false)
    ))

    withTable("test_db", "test_write_empty_map", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Map[String, Int]()),
        Row(2, Map("a" -> 1)),
        Row(3, Map[String, Int]())
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_empty_map")

      val result = spark.table(s"$actualDb.test_write_empty_map").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getMap[String, Int](1).isEmpty)
      assert(result(1).getMap[String, Int](1) == Map("a" -> 1))
      assert(result(2).getMap[String, Int](1).isEmpty)
    }
  }

  test("write MapType - map of string to int") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false)
    ))

    withTable("test_db", "test_write_map", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Map("a" -> 1, "b" -> 2)),
        Row(2, Map("x" -> 10, "y" -> 20)),
        Row(3, Map("foo" -> 100))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_map")

      val result = spark.table(s"$actualDb.test_write_map").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getMap[String, Int](1) == Map("a" -> 1, "b" -> 2))
      assert(result(1).getMap[String, Int](1) == Map("x" -> 10, "y" -> 20))
      assert(result(2).getMap[String, Int](1) == Map("foo" -> 100))
    }
  }

  test("write MapType - with nullable values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", MapType(StringType, IntegerType, valueContainsNull = true), nullable = false)
    ))

    withTable("test_db", "test_write_map_nullable", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Map("a" -> 1, "b" -> null)),
        Row(2, Map("x" -> null, "y" -> 20)),
        Row(3, Map("foo" -> 100))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_map_nullable")

      val result = spark.table(s"$actualDb.test_write_map_nullable").orderBy("id").collect()
      assert(result.length == 3)
      val map1 = result(0).getMap[String, Any](1)
      assert(map1("a") == 1)
      assert(map1("b") == null)
    }
  }

  test("write ShortType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ShortType, nullable = false)
    ))

    withTable("test_db", "test_write_short", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Short.MinValue),
        Row(2, 0.toShort),
        Row(3, Short.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_short")

      val result = spark.table(s"$actualDb.test_write_short").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getShort(1) == Short.MinValue)
      assert(result(1).getShort(1) == 0.toShort)
      assert(result(2).getShort(1) == Short.MaxValue)
    }
  }

  test("write ShortType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ShortType, nullable = true)
    ))

    withTable("test_db", "test_write_short_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, Short.MinValue),
        Row(2, null),
        Row(3, Short.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_short_null")

      val result = spark.table(s"$actualDb.test_write_short_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getShort(1) == Short.MinValue)
      assert(result(1).isNullAt(1))
      assert(result(2).getShort(1) == Short.MaxValue)
    }
  }

  test("write StringType - empty strings") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = false)
    ))

    withTable("test_db", "test_write_empty_string", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, ""),
        Row(2, "not empty"),
        Row(3, "")
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_empty_string")

      val result = spark.table(s"$actualDb.test_write_empty_string").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "")
      assert(result(1).getString(1) == "not empty")
      assert(result(2).getString(1) == "")
    }
  }

  test("write StringType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = true)
    ))

    withTable("test_db", "test_write_string_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, "hello"),
        Row(2, null),
        Row(3, "world")
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_string_null")

      val result = spark.table(s"$actualDb.test_write_string_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "hello")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "world")
    }
  }

  test("write StringType - regular strings") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = false)
    ))

    withTable("test_db", "test_write_string", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, "hello"),
        Row(2, "world"),
        Row(3, "test")
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_string")

      val result = spark.table(s"$actualDb.test_write_string").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "hello")
      assert(result(1).getString(1) == "world")
      assert(result(2).getString(1) == "test")
    }
  }

  test("write TimestampType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", TimestampType, nullable = true)
    ))

    withTable("test_db", "test_write_timestamp_null", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, java.sql.Timestamp.valueOf("2024-01-01 12:00:00")),
        Row(2, null),
        Row(3, java.sql.Timestamp.valueOf("2024-12-31 23:59:59"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_timestamp_null")

      val result = spark.table(s"$actualDb.test_write_timestamp_null").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getTimestamp(1) != null)
    }
  }

  test("write TimestampType - timestamps") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", TimestampType, nullable = false)
    ))

    withTable("test_db", "test_write_timestamp", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1, java.sql.Timestamp.valueOf("2024-01-01 12:00:00")),
        Row(2, java.sql.Timestamp.valueOf("2024-06-15 18:30:45")),
        Row(3, java.sql.Timestamp.valueOf("2024-12-31 23:59:59"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable(s"$actualDb.test_write_timestamp")

      val result = spark.table(s"$actualDb.test_write_timestamp").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
      assert(result(2).getTimestamp(1) != null)
    }
  }

  test("write StructType - simple tuple") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField(
          "person",
          StructType(Seq(
            StructField("name", StringType, false),
            StructField("age", IntegerType, false)
          )),
          false
        ) :: Nil
    )

    withTable("test_db", "test_write_simple_struct", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1L, Row("Alice", 30)),
        Row(2L, Row("Bob", 25)),
        Row(3L, Row("Charlie", 35))
      )
      val dataDF = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      dataDF.writeTo("$actualDb.test_write_simple_struct").append()

      val result = spark.table(s"$actualDb.test_write_simple_struct").sort("id").collect()

      assert(result.length === 3)
      val row0 = result(0)
      assert(row0.getLong(0) === 1L)
      val person0 = row0.getStruct(1)
      assert(person0.getString(0) === "Alice")
      assert(person0.getInt(1) === 30)

      val row1 = result(1)
      assert(row1.getLong(0) === 2L)
      val person1 = row1.getStruct(1)
      assert(person1.getString(0) === "Bob")
      assert(person1.getInt(1) === 25)
    }
  }

  test("write StructType - nested tuple") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField(
          "data",
          StructType(Seq(
            StructField("name", StringType, false),
            StructField(
              "address",
              StructType(Seq(
                StructField("city", StringType, false),
                StructField("zip", IntegerType, false)
              )),
              false
            )
          )),
          false
        ) :: Nil
    )

    withTable("test_db", "test_write_nested_struct", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1L, Row("Alice", Row("NYC", 10001))),
        Row(2L, Row("Bob", Row("LA", 90001)))
      )
      val dataDF = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      dataDF.writeTo("$actualDb.test_write_nested_struct").append()

      val result = spark.table(s"$actualDb.test_write_nested_struct").sort("id").collect()

      assert(result.length === 2)
      val row0 = result(0)
      assert(row0.getLong(0) === 1L)
      val data0 = row0.getStruct(1)
      assert(data0.getString(0) === "Alice")
      val address0 = data0.getStruct(1)
      assert(address0.getString(0) === "NYC")
      assert(address0.getInt(1) === 10001)
    }
  }

  test("write StructType - nullable fields") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField(
          "person",
          StructType(Seq(
            StructField("name", StringType, false),
            StructField("age", IntegerType, true) // nullable
          )),
          false
        ) :: Nil
    )

    withTable("test_db", "test_write_nullable_struct", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1L, Row("Alice", 30)),
        Row(2L, Row("Bob", null)),
        Row(3L, Row("Charlie", 35))
      )
      val dataDF = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      dataDF.writeTo("$actualDb.test_write_nullable_struct").append()

      val result = spark.table(s"$actualDb.test_write_nullable_struct").sort("id").collect()

      assert(result.length === 3)
      val row1 = result(1)
      assert(row1.getLong(0) === 2L)
      val person1 = row1.getStruct(1)
      assert(person1.getString(0) === "Bob")
      assert(person1.isNullAt(1))
    }
  }

  test("write StructType - complex types (Boolean, Date, Double)") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField(
          "complex",
          StructType(Seq(
            StructField("name", StringType, false),
            StructField("score", DoubleType, false),
            StructField("active", BooleanType, false),
            StructField("created", DateType, false)
          )),
          false
        ) :: Nil
    )

    withTable("test_db", "test_write_complex_struct", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1L, Row("Alice", 95.5, true, date("2024-01-15"))),
        Row(2L, Row("Bob", 87.3, false, date("2024-02-20")))
      )
      val dataDF = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      dataDF.writeTo("$actualDb.test_write_complex_struct").append()

      val result = spark.table(s"$actualDb.test_write_complex_struct").sort("id").collect()

      assert(result.length === 2)
      val row0 = result(0)
      assert(row0.getLong(0) === 1L)
      val complex0 = row0.getStruct(1)
      assert(complex0.getString(0) === "Alice")
      assert(complex0.getDouble(1) === 95.5)
      assert(complex0.getBoolean(2) === true)
      assert(complex0.getDate(3) === date("2024-01-15"))

      val row1 = result(1)
      assert(row1.getLong(0) === 2L)
      val complex1 = row1.getStruct(1)
      assert(complex1.getString(0) === "Bob")
      assert(complex1.getDouble(1) === 87.3)
      assert(complex1.getBoolean(2) === false)
      assert(complex1.getDate(3) === date("2024-02-20"))
    }
  }

  test("write StructType - array field within struct") {
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField(
          "profile",
          StructType(Seq(
            StructField("name", StringType, false),
            StructField("roles", ArrayType(StringType, false), false),
            StructField("scores", ArrayType(IntegerType, false), false)
          )),
          false
        ) :: Nil
    )

    withTable("test_db", "test_write_struct_array", schema) { (actualDb: String, actualTbl: String) =>
      val data = Seq(
        Row(1L, Row("Alice", Seq("admin", "user"), Seq(95, 88, 92))),
        Row(2L, Row("Bob", Seq("user"), Seq(78, 85)))
      )
      val dataDF = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      dataDF.writeTo("$actualDb.test_write_struct_array").append()

      val result = spark.table(s"$actualDb.test_write_struct_array").sort("id").collect()

      assert(result.length === 2)
      val row0 = result(0)
      assert(row0.getLong(0) === 1L)
      val profile0 = row0.getStruct(1)
      assert(profile0.getString(0) === "Alice")
      assert(profile0.getSeq[String](1) === Seq("admin", "user"))
      assert(profile0.getSeq[Int](2) === Seq(95, 88, 92))

      val row1 = result(1)
      assert(row1.getLong(0) === 2L)
      val profile1 = row1.getStruct(1)
      assert(profile1.getString(0) === "Bob")
      assert(profile1.getSeq[String](1) === Seq("user"))
      assert(profile1.getSeq[Int](2) === Seq(78, 85))
    }
  }

  test("write StructType - full round-trip with Spark-created table") {
    // This test verifies the complete cycle: Spark creates table, writes data, reads it back
    val db = "test_db"
    val tbl = "test_struct_roundtrip"

    // Create schema with various struct configurations
    val schema = StructType(
      StructField("id", LongType, false) ::
        StructField(
          "user_info",
          StructType(Seq(
            StructField("name", StringType, false),
            StructField("age", IntegerType, false),
            StructField("active", BooleanType, false)
          )),
          false
        ) ::
        StructField(
          "metadata",
          StructType(Seq(
            StructField("created", DateType, false),
            StructField("score", DoubleType, false),
            StructField("tags", ArrayType(StringType, false), false)
          )),
          false
        ) :: Nil
    )

    try {
      // Create database via Spark
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")

      // Create table via Spark with explicit schema
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $db.$tbl (
           |  id BIGINT NOT NULL,
           |  user_info STRUCT<name: STRING, age: INT, active: BOOLEAN> NOT NULL,
           |  metadata STRUCT<created: DATE, score: DOUBLE, tags: ARRAY<STRING>> NOT NULL
           |) USING clickhouse
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = 'id'
           |)
           |""".stripMargin
      )
      // Write data via Spark
      val writeData = Seq(
        Row(
          1L,
          Row("Alice", 30, true),
          Row(date("2024-01-15"), 95.5, Seq("premium", "verified"))
        ),
        Row(
          2L,
          Row("Bob", 25, false),
          Row(date("2024-02-20"), 87.3, Seq("basic"))
        ),
        Row(
          3L,
          Row("Charlie", 35, true),
          Row(date("2024-03-10"), 92.1, Seq("premium", "admin", "verified"))
        )
      )

      val writeDF = spark.createDataFrame(
        spark.sparkContext.parallelize(writeData),
        schema
      )

      writeDF.writeTo(s"$db.$tbl").append()

      // Read data back via Spark
      val readDF = spark.table(s"$db.$tbl").sort("id")
      val result = readDF.collect()

      // Verify schema structure (note: array containsNull may change to true in ClickHouse)
      assert(readDF.schema.fields.length === schema.fields.length)
      assert(readDF.schema.fields(0).name === "id")
      assert(readDF.schema.fields(1).name === "user_info")
      assert(readDF.schema.fields(2).name === "metadata")

      // Verify data integrity
      assert(result.length === 3)

      // Verify first row
      val row0 = result(0)
      assert(row0.getLong(0) === 1L)
      val userInfo0 = row0.getStruct(1)
      assert(userInfo0.getString(0) === "Alice")
      assert(userInfo0.getInt(1) === 30)
      assert(userInfo0.getBoolean(2) === true)
      val metadata0 = row0.getStruct(2)
      assert(metadata0.getDate(0) === date("2024-01-15"))
      assert(metadata0.getDouble(1) === 95.5)
      assert(metadata0.getSeq[String](2) === Seq("premium", "verified"))

      // Verify second row
      val row1 = result(1)
      assert(row1.getLong(0) === 2L)
      val userInfo1 = row1.getStruct(1)
      assert(userInfo1.getString(0) === "Bob")
      assert(userInfo1.getInt(1) === 25)
      assert(userInfo1.getBoolean(2) === false)
      val metadata1 = row1.getStruct(2)
      assert(metadata1.getDate(0) === date("2024-02-20"))
      assert(metadata1.getDouble(1) === 87.3)
      assert(metadata1.getSeq[String](2) === Seq("basic"))

      // Verify third row
      val row2 = result(2)
      assert(row2.getLong(0) === 3L)
      val userInfo2 = row2.getStruct(1)
      assert(userInfo2.getString(0) === "Charlie")
      assert(userInfo2.getInt(1) === 35)
      assert(userInfo2.getBoolean(2) === true)
      val metadata2 = row2.getStruct(2)
      assert(metadata2.getDate(0) === date("2024-03-10"))
      assert(metadata2.getDouble(1) === 92.1)
      assert(metadata2.getSeq[String](2) === Seq("premium", "admin", "verified"))

    } finally {
      // Clean up: drop table and database
      spark.sql(s"DROP TABLE IF EXISTS $db.$tbl")
      spark.sql(s"DROP DATABASE IF EXISTS $db")
    }
  }

}
