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

  // Test: BooleanType writing
  // NOTE: ClickHouse stores Boolean as UInt8, so it reads back as Short (0 or 1)
  test("write BooleanType - true and false values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", BooleanType, nullable = false)
    ))
    
    withTable("test_db", "test_write_bool", schema) {
      val data = Seq(
        Row(1, true),
        Row(2, false)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_bool")
      
      val result = spark.table("test_db.test_write_bool").orderBy("id").collect()
      assert(result.length == 2)
      // Boolean is stored as UInt8 in ClickHouse, reads back as Short
      assert(result(0).getShort(1) == 1)
      assert(result(1).getShort(1) == 0)
    }
  }

  test("write BooleanType - nullable with null values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", BooleanType, nullable = true)
    ))
    
    withTable("test_db", "test_write_bool_null", schema) {
      val data = Seq(
        Row(1, true),
        Row(2, null),
        Row(3, false)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_bool_null")
      
      val result = spark.table("test_db.test_write_bool_null").orderBy("id").collect()
      assert(result.length == 3)
      // Boolean is stored as UInt8 in ClickHouse, reads back as Short
      assert(result(0).getShort(1) == 1)
      assert(result(1).isNullAt(1))
      assert(result(2).getShort(1) == 0)
    }
  }

  // Test: ByteType writing
  test("write ByteType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ByteType, nullable = false)
    ))
    
    withTable("test_db", "test_write_byte", schema) {
      val data = Seq(
        Row(1, Byte.MinValue),
        Row(2, 0.toByte),
        Row(3, Byte.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_byte")
      
      val result = spark.table("test_db.test_write_byte").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getByte(1) == Byte.MinValue)
      assert(result(1).getByte(1) == 0.toByte)
      assert(result(2).getByte(1) == Byte.MaxValue)
    }
  }

  // Test: ShortType writing
  test("write ShortType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ShortType, nullable = false)
    ))
    
    withTable("test_db", "test_write_short", schema) {
      val data = Seq(
        Row(1, Short.MinValue),
        Row(2, 0.toShort),
        Row(3, Short.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_short")
      
      val result = spark.table("test_db.test_write_short").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getShort(1) == Short.MinValue)
      assert(result(1).getShort(1) == 0.toShort)
      assert(result(2).getShort(1) == Short.MaxValue)
    }
  }

  // Test: IntegerType writing
  test("write IntegerType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = false)
    ))
    
    withTable("test_db", "test_write_int", schema) {
      val data = Seq(
        Row(1, Int.MinValue),
        Row(2, 0),
        Row(3, Int.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_int")
      
      val result = spark.table("test_db.test_write_int").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getInt(1) == Int.MinValue)
      assert(result(1).getInt(1) == 0)
      assert(result(2).getInt(1) == Int.MaxValue)
    }
  }

  // Test: LongType writing
  test("write LongType - min and max values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", LongType, nullable = false)
    ))
    
    withTable("test_db", "test_write_long", schema) {
      val data = Seq(
        Row(1, Long.MinValue),
        Row(2, 0L),
        Row(3, Long.MaxValue)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_long")
      
      val result = spark.table("test_db.test_write_long").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getLong(1) == Long.MinValue)
      assert(result(1).getLong(1) == 0L)
      assert(result(2).getLong(1) == Long.MaxValue)
    }
  }

  // Test: FloatType writing
  test("write FloatType - regular values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", FloatType, nullable = false)
    ))
    
    withTable("test_db", "test_write_float", schema) {
      val data = Seq(
        Row(1, 3.14f),
        Row(2, -2.718f),
        Row(3, 0.0f)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_float")
      
      val result = spark.table("test_db.test_write_float").orderBy("id").collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getFloat(1) - 3.14f) < 0.001f)
      assert(math.abs(result(1).getFloat(1) - (-2.718f)) < 0.001f)
      assert(result(2).getFloat(1) == 0.0f)
    }
  }

  // Test: DoubleType writing
  test("write DoubleType - regular values") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DoubleType, nullable = false)
    ))
    
    withTable("test_db", "test_write_double", schema) {
      val data = Seq(
        Row(1, 3.14159),
        Row(2, -2.71828),
        Row(3, 0.0)
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_double")
      
      val result = spark.table("test_db.test_write_double").orderBy("id").collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDouble(1) - 3.14159) < 0.00001)
      assert(math.abs(result(1).getDouble(1) - (-2.71828)) < 0.00001)
      assert(result(2).getDouble(1) == 0.0)
    }
  }

  // Test: DecimalType writing
  test("write DecimalType - Decimal(10,2)") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DecimalType(10, 2), nullable = false)
    ))
    
    withTable("test_db", "test_write_decimal", schema) {
      val data = Seq(
        Row(1, BigDecimal("12345.67")),
        Row(2, BigDecimal("-9999.99")),
        Row(3, BigDecimal("0.01"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_decimal")
      
      val result = spark.table("test_db.test_write_decimal").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1) == BigDecimal("12345.67").underlying())
      assert(result(1).getDecimal(1) == BigDecimal("-9999.99").underlying())
      assert(result(2).getDecimal(1) == BigDecimal("0.01").underlying())
    }
  }

  // Test: StringType writing
  test("write StringType - regular strings") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = false)
    ))
    
    withTable("test_db", "test_write_string", schema) {
      val data = Seq(
        Row(1, "hello"),
        Row(2, "world"),
        Row(3, "test")
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_string")
      
      val result = spark.table("test_db.test_write_string").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "hello")
      assert(result(1).getString(1) == "world")
      assert(result(2).getString(1) == "test")
    }
  }

  test("write StringType - empty strings") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = false)
    ))
    
    withTable("test_db", "test_write_empty_string", schema) {
      val data = Seq(
        Row(1, ""),
        Row(2, "not empty"),
        Row(3, "")
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_empty_string")
      
      val result = spark.table("test_db.test_write_empty_string").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "")
      assert(result(1).getString(1) == "not empty")
      assert(result(2).getString(1) == "")
    }
  }

  // Test: DateType writing
  test("write DateType - dates") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DateType, nullable = false)
    ))
    
    withTable("test_db", "test_write_date", schema) {
      val data = Seq(
        Row(1, java.sql.Date.valueOf("2024-01-01")),
        Row(2, java.sql.Date.valueOf("2024-06-15")),
        Row(3, java.sql.Date.valueOf("2024-12-31"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_date")
      
      val result = spark.table("test_db.test_write_date").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).getDate(1) != null)
      assert(result(2).getDate(1) != null)
    }
  }

  // Test: TimestampType writing
  test("write TimestampType - timestamps") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", TimestampType, nullable = false)
    ))
    
    withTable("test_db", "test_write_timestamp", schema) {
      val data = Seq(
        Row(1, java.sql.Timestamp.valueOf("2024-01-01 12:00:00")),
        Row(2, java.sql.Timestamp.valueOf("2024-06-15 18:30:45")),
        Row(3, java.sql.Timestamp.valueOf("2024-12-31 23:59:59"))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_timestamp")
      
      val result = spark.table("test_db.test_write_timestamp").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
      assert(result(2).getTimestamp(1) != null)
    }
  }

  // Test: ArrayType writing
  test("write ArrayType - array of integers") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", ArrayType(IntegerType, containsNull = false), nullable = false)
    ))
    
    withTable("test_db", "test_write_array_int", schema) {
      val data = Seq(
        Row(1, Seq(1, 2, 3)),
        Row(2, Seq(10, 20, 30)),
        Row(3, Seq(100))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_array_int")
      
      val result = spark.table("test_db.test_write_array_int").orderBy("id").collect()
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
    
    withTable("test_db", "test_write_empty_array", schema) {
      val data = Seq(
        Row(1, Seq()),
        Row(2, Seq(1, 2, 3)),
        Row(3, Seq())
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_empty_array")
      
      val result = spark.table("test_db.test_write_empty_array").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1).isEmpty)
      assert(result(1).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(2).getSeq[Int](1).isEmpty)
    }
  }

  // Test: MapType writing
  test("write MapType - map of string to int") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false)
    ))
    
    withTable("test_db", "test_write_map", schema) {
      val data = Seq(
        Row(1, Map("a" -> 1, "b" -> 2)),
        Row(2, Map("x" -> 10, "y" -> 20)),
        Row(3, Map("foo" -> 100))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_map")
      
      val result = spark.table("test_db.test_write_map").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getMap[String, Int](1) == Map("a" -> 1, "b" -> 2))
      assert(result(1).getMap[String, Int](1) == Map("x" -> 10, "y" -> 20))
      assert(result(2).getMap[String, Int](1) == Map("foo" -> 100))
    }
  }

  test("write MapType - empty maps") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false)
    ))
    
    withTable("test_db", "test_write_empty_map", schema) {
      val data = Seq(
        Row(1, Map[String, Int]()),
        Row(2, Map("a" -> 1)),
        Row(3, Map[String, Int]())
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      df.write.mode(SaveMode.Append).saveAsTable("test_db.test_write_empty_map")
      
      val result = spark.table("test_db.test_write_empty_map").orderBy("id").collect()
      assert(result.length == 3)
      assert(result(0).getMap[String, Int](1).isEmpty)
      assert(result(1).getMap[String, Int](1) == Map("a" -> 1))
      assert(result(2).getMap[String, Int](1).isEmpty)
    }
  }

}
