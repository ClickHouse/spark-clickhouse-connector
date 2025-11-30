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
import org.apache.spark.sql.types._

/**
 * Shared test cases for both JSON and Binary readers.
 * Subclasses only need to configure the read format.
 * 
 * Tests are organized by ClickHouse data type with both regular and nullable variants.
 * Each type includes comprehensive coverage of edge cases and null handling.
 */
trait ClickHouseReaderTestBase extends SparkClickHouseSingleTest {

  // ============================================================================
  // ArrayType Tests
  // ============================================================================

  test("decode ArrayType - Array of integers") {
    withKVTable("test_db", "test_array_int", valueColDef = "Array(Int32)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_array_int VALUES
           |(1, [1, 2, 3]),
           |(2, []),
           |(3, [100, 200, 300, 400])
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_array_int ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(1).getSeq[Int](1) == Seq())
      assert(result(2).getSeq[Int](1) == Seq(100, 200, 300, 400))
    }
  }
  test("decode ArrayType - Array of strings") {
    withKVTable("test_db", "test_array_string", valueColDef = "Array(String)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_array_string VALUES
             |(1, ['hello', 'world']),
             |(2, []),
             |(3, ['a', 'b', 'c'])
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_array_string ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getSeq[String](1) == Seq("hello", "world"))
        assert(result(1).getSeq[String](1) == Seq())
        assert(result(2).getSeq[String](1) == Seq("a", "b", "c"))
    }
  }
  test("decode ArrayType - Array with nullable elements") {
    withKVTable("test_db", "test_array_nullable", valueColDef = "Array(Nullable(Int32))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_array_nullable VALUES
             |(1, [1, NULL, 3]),
             |(2, [NULL, NULL]),
             |(3, [100, 200])
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_array_nullable ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        // Verify arrays can be read
        assert(result(0).getSeq[Any](1) != null)
        assert(result(1).getSeq[Any](1) != null)
        assert(result(2).getSeq[Any](1) != null)
    }
  }
  test("decode ArrayType - empty arrays") {
    withKVTable("test_db", "test_empty_array", valueColDef = "Array(Int32)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_empty_array VALUES
           |(1, []),
           |(2, [1, 2, 3]),
           |(3, [])
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_empty_array ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1).isEmpty)
      assert(result(1).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(2).getSeq[Int](1).isEmpty)
    }
  }
  test("decode ArrayType - Nested arrays") {
    withKVTable("test_db", "test_nested_array", valueColDef = "Array(Array(Int32))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_nested_array VALUES
             |(1, [[1, 2], [3, 4]]),
             |(2, [[], [5]]),
             |(3, [[10, 20, 30]])
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_nested_array ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        // Verify nested arrays can be read
        assert(result(0).get(1) != null)
        assert(result(1).get(1) != null)
        assert(result(2).get(1) != null)
    }
  }
  test("decode BinaryType - FixedString") {
    // FixedString is read as String by default in the connector
    withKVTable("test_db", "test_fixedstring", valueColDef = "FixedString(5)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_fixedstring VALUES
             |(1, 'hello'),
             |(2, 'world')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_fixedstring ORDER BY key")
        val result = df.collect()
        assert(result.length == 2)
        // FixedString should be readable
        assert(result(0).get(1) != null)
        assert(result(1).get(1) != null)
    }
  }
  test("decode BinaryType - FixedString nullable with null values") {
    withKVTable("test_db", "test_fixedstring_null", valueColDef = "Nullable(FixedString(5))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_fixedstring_null VALUES
             |(1, 'hello'),
             |(2, NULL),
             |(3, 'world')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_fixedstring_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).get(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).get(1) != null)
    }
  }

  // ============================================================================
  // BooleanType Tests
  // ============================================================================

  test("decode BooleanType - true and false values") {
    // ClickHouse Bool now correctly maps to BooleanType
    withKVTable("test_db", "test_bool", valueColDef = "Bool") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_bool VALUES
           |(1, true),
           |(2, false),
           |(3, 1),
           |(4, 0)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_bool ORDER BY key")
      val result = df.collect()
      assert(result.length == 4)
      // Check the value - handle both Boolean (JSON) and Short (Binary) formats
      val v0 = result(0).get(1)
      val v1 = result(1).get(1)
      v0 match {
        case b: Boolean =>
          assert(b == true)
          assert(result(1).getBoolean(1) == false)
          assert(result(2).getBoolean(1) == true)
          assert(result(3).getBoolean(1) == false)
        case s: Short =>
          assert(s == 1)
          assert(result(1).getShort(1) == 0)
          assert(result(2).getShort(1) == 1)
          assert(result(3).getShort(1) == 0)
        case _ => fail(s"Unexpected type: ${v0.getClass}")
      }
    }
  }
  test("decode BooleanType - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_bool_null", valueColDef = "Nullable(Bool)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_bool_null VALUES
           |(1, true),
           |(2, NULL),
           |(3, false)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_bool_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(1).isNullAt(1))
      // Check the value - handle both Boolean (JSON) and Short (Binary) formats
      val v0 = result(0).get(1)
      v0 match {
        case b: Boolean =>
          assert(b == true)
          assert(result(2).getBoolean(1) == false)
        case s: Short =>
          assert(s == 1)
          assert(result(2).getShort(1) == 0)
        case _ => fail(s"Unexpected type: ${v0.getClass}")
      }
    }
  }

  // ============================================================================
  // ByteType Tests
  // ============================================================================

  test("decode ByteType - min and max values") {
    withKVTable("test_db", "test_byte", valueColDef = "Int8") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_byte VALUES
           |(1, -128),
           |(2, 0),
           |(3, 127)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_byte ORDER BY key")
      checkAnswer(
        df,
        Row(1, -128.toByte) :: Row(2, 0.toByte) :: Row(3, 127.toByte) :: Nil
      )
    }
  }
  test("decode ByteType - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_byte_null", valueColDef = "Nullable(Int8)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_byte_null VALUES
           |(1, -128),
           |(2, NULL),
           |(3, 127)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_byte_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -128.toByte) :: Row(2, null) :: Row(3, 127.toByte) :: Nil
      )
    }
  }
  test("decode DateTime32 - 32-bit timestamp") {
    withKVTable("test_db", "test_datetime32", valueColDef = "DateTime32") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_datetime32 VALUES
           |(1, '2024-01-01 12:00:00'),
           |(2, '2024-06-15 18:30:45')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_datetime32 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
    }
  }
  test("decode DateTime32 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_datetime32_null", valueColDef = "Nullable(DateTime32)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_datetime32_null VALUES
             |(1, '2024-01-01 12:00:00'),
             |(2, NULL),
             |(3, '2024-06-15 18:30:45')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_datetime32_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getTimestamp(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode DateType - Date") {
    withKVTable("test_db", "test_date", valueColDef = "Date") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_date VALUES
           |(1, '2024-01-01'),
           |(2, '2024-06-15'),
           |(3, '2024-12-31')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_date ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).getDate(1) != null)
      assert(result(2).getDate(1) != null)
    }
  }
  test("decode DateType - Date32") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_date32", valueColDef = "Date32") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_date32 VALUES
           |(1, '1900-01-01'),
           |(2, '2024-06-15'),
           |(3, '2100-12-31')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_date32 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).getDate(1) != null)
      assert(result(2).getDate(1) != null)
    }
  }
  test("decode DateType - Date32 nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_date32_null", valueColDef = "Nullable(Date32)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_date32_null VALUES
             |(1, '1900-01-01'),
             |(2, NULL),
             |(3, '2100-12-31')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_date32_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDate(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getDate(1) != null)
    }
  }
  test("decode DateType - nullable with null values") {
    withKVTable("test_db", "test_date_null", valueColDef = "Nullable(Date)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_date_null VALUES
           |(1, '2024-01-01'),
           |(2, NULL),
           |(3, '2024-12-31')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_date_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getDate(1) != null)
    }
  }
  test("decode DecimalType - Decimal128") {
    // Decimal128(20) means scale=20, max precision=38 total digits
    // Use values with max 18 digits before decimal to stay within 38 total
    withKVTable("test_db", "test_decimal128", valueColDef = "Decimal128(20)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_decimal128 VALUES
           |(1, 123456789012345.12345678901234567890),
           |(2, -999999999999999.99999999999999999999),
           |(3, 0.00000000000000000001)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_decimal128 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      // Decimal128(20) means 20 decimal places, total precision up to 38 digits
      assert(math.abs(result(0).getDecimal(1).doubleValue() - 123456789012345.12345678901234567890) < 0.01)
      assert(math.abs(result(1).getDecimal(1).doubleValue() - -999999999999999.99999999999999999999) < 0.01)
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode DecimalType - Decimal128 nullable with null values") {
    withKVTable("test_db", "test_decimal128_null", valueColDef = "Nullable(Decimal128(20))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_decimal128_null VALUES
             |(1, 123456789012345.12345678901234567890),
             |(2, NULL),
             |(3, 0.00000000000000000001)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_decimal128_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode DecimalType - Decimal32") {
    withKVTable("test_db", "test_decimal32", valueColDef = "Decimal32(4)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_decimal32 VALUES
           |(1, 12345.6789),
           |(2, -9999.9999),
           |(3, 0.0001)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_decimal32 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).doubleValue() == 12345.6789)
      assert(result(1).getDecimal(1).doubleValue() == -9999.9999)
      assert(result(2).getDecimal(1).doubleValue() == 0.0001)
    }
  }
  test("decode DecimalType - Decimal32 nullable with null values") {
    withKVTable("test_db", "test_decimal32_null", valueColDef = "Nullable(Decimal32(4))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_decimal32_null VALUES
             |(1, 12345.6789),
             |(2, NULL),
             |(3, 0.0001)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_decimal32_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode DecimalType - Decimal64") {
    // Decimal64(10) means scale=10, max precision=18 total digits
    // Use values with max 8 digits before decimal to stay within 18 total
    withKVTable("test_db", "test_decimal64", valueColDef = "Decimal64(10)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_decimal64 VALUES
           |(1, 1234567.0123456789),
           |(2, -9999999.9999999999),
           |(3, 0.0000000001)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_decimal64 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDecimal(1).doubleValue() - 1234567.0123456789) < 0.0001)
      assert(math.abs(result(1).getDecimal(1).doubleValue() - -9999999.9999999999) < 0.0001)
      assert(math.abs(result(2).getDecimal(1).doubleValue() - 0.0000000001) < 0.0000000001)
    }
  }
  test("decode DecimalType - Decimal64 nullable with null values") {
    withKVTable("test_db", "test_decimal64_null", valueColDef = "Nullable(Decimal64(10))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_decimal64_null VALUES
             |(1, 1234567.0123456789),
             |(2, NULL),
             |(3, 0.0000000001)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_decimal64_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode DoubleType - nullable with null values") {
    withKVTable("test_db", "test_double_null", valueColDef = "Nullable(Float64)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_double_null VALUES
             |(1, 1.23),
             |(2, NULL),
             |(3, -4.56)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_double_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(math.abs(result(0).getDouble(1) - 1.23) < 0.0001)
        assert(result(1).isNullAt(1))
        assert(math.abs(result(2).getDouble(1) - -4.56) < 0.0001)
    }
  }
  test("decode DoubleType - regular values") {
    withKVTable("test_db", "test_double", valueColDef = "Float64") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_double VALUES
           |(1, -3.141592653589793),
           |(2, 0.0),
           |(3, 3.141592653589793)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_double ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDouble(1) - -3.141592653589793) < 0.000001)
      assert(result(1).getDouble(1) == 0.0)
      assert(math.abs(result(2).getDouble(1) - 3.141592653589793) < 0.000001)
    }
  }
  test("decode Enum16 - large enum") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_enum16", valueColDef = "Enum16('small' = 1, 'medium' = 100, 'large' = 1000)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_enum16 VALUES
             |(1, 'small'),
             |(2, 'medium'),
             |(3, 'large')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_enum16 ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getString(1) == "small")
        assert(result(1).getString(1) == "medium")
        assert(result(2).getString(1) == "large")
    }
  }
  test("decode Enum16 - nullable with null values") {
    withKVTable(
      "test_db",
      "test_enum16_null",
      valueColDef = "Nullable(Enum16('small' = 1, 'medium' = 100, 'large' = 1000))"
    ) { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_enum16_null VALUES
           |(1, 'small'),
           |(2, NULL),
           |(3, 'large')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_enum16_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "small")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "large")
    }
  }
  test("decode Enum8 - nullable with null values") {
    withKVTable("test_db", "test_enum8_null", valueColDef = "Nullable(Enum8('red' = 1, 'green' = 2, 'blue' = 3))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_enum8_null VALUES
             |(1, 'red'),
             |(2, NULL),
             |(3, 'blue')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_enum8_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getString(1) == "red")
        assert(result(1).isNullAt(1))
        assert(result(2).getString(1) == "blue")
    }
  }
  test("decode Enum8 - small enum") {
    withKVTable("test_db", "test_enum8", valueColDef = "Enum8('red' = 1, 'green' = 2, 'blue' = 3)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_enum8 VALUES
             |(1, 'red'),
             |(2, 'green'),
             |(3, 'blue')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_enum8 ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getString(1) == "red")
        assert(result(1).getString(1) == "green")
        assert(result(2).getString(1) == "blue")
    }
  }
  test("decode FloatType - nullable with null values") {
    withKVTable("test_db", "test_float_null", valueColDef = "Nullable(Float32)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_float_null VALUES
             |(1, 1.5),
             |(2, NULL),
             |(3, -2.5)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_float_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(math.abs(result(0).getFloat(1) - 1.5f) < 0.01f)
        assert(result(1).isNullAt(1))
        assert(math.abs(result(2).getFloat(1) - -2.5f) < 0.01f)
    }
  }
  test("decode FloatType - regular values") {
    withKVTable("test_db", "test_float", valueColDef = "Float32") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_float VALUES
           |(1, -3.14),
           |(2, 0.0),
           |(3, 3.14)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_float ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getFloat(1) - -3.14f) < 0.01f)
      assert(result(1).getFloat(1) == 0.0f)
      assert(math.abs(result(2).getFloat(1) - 3.14f) < 0.01f)
    }
  }
  test("decode Int128 - large integers as Decimal") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_int128", valueColDef = "Int128") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_int128 VALUES
           |(1, 0),
           |(2, 123456789012345678901234567890),
           |(3, -123456789012345678901234567890)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_int128 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode Int128 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_int128_null", valueColDef = "Nullable(Int128)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_int128_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, -123456789012345678901234567890)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_int128_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode Int256 - nullable with null values") {
    withKVTable("test_db", "test_int256_null", valueColDef = "Nullable(Int256)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_int256_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 12345678901234567890123456789012345678)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_int256_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode Int256 - very large integers as Decimal") {
    withKVTable("test_db", "test_int256", valueColDef = "Int256") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_int256 VALUES
           |(1, 0),
           |(2, 12345678901234567890123456789012345678)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_int256 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
    }
  }
  test("decode IntegerType - min and max values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_int", valueColDef = "Int32") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_int VALUES
           |(1, -2147483648),
           |(2, 0),
           |(3, 2147483647)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_int ORDER BY key")
      checkAnswer(
        df,
        Row(1, -2147483648) :: Row(2, 0) :: Row(3, 2147483647) :: Nil
      )
    }
  }
  test("decode IntegerType - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_int_null", valueColDef = "Nullable(Int32)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_int_null VALUES
           |(1, -2147483648),
           |(2, NULL),
           |(3, 2147483647)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_int_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -2147483648) :: Row(2, null) :: Row(3, 2147483647) :: Nil
      )
    }
  }
  test("decode IPv4 - IP addresses") {
    withKVTable("test_db", "test_ipv4", valueColDef = "IPv4") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_ipv4 VALUES
           |(1, '127.0.0.1'),
           |(2, '192.168.1.1'),
           |(3, '8.8.8.8')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_ipv4 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "127.0.0.1")
      assert(result(1).getString(1) == "192.168.1.1")
      assert(result(2).getString(1) == "8.8.8.8")
    }
  }
  test("decode IPv4 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_ipv4_null", valueColDef = "Nullable(IPv4)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_ipv4_null VALUES
           |(1, '127.0.0.1'),
           |(2, NULL),
           |(3, '8.8.8.8')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_ipv4_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "127.0.0.1")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "8.8.8.8")
    }
  }
  test("decode IPv6 - IPv6 addresses") {
    withKVTable("test_db", "test_ipv6", valueColDef = "IPv6") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_ipv6 VALUES
           |(1, '::1'),
           |(2, '2001:0db8:85a3:0000:0000:8a2e:0370:7334'),
           |(3, 'fe80::1')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_ipv6 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) != null)
      assert(result(1).getString(1) != null)
      assert(result(2).getString(1) != null)
    }
  }
  test("decode IPv6 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_ipv6_null", valueColDef = "Nullable(IPv6)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_ipv6_null VALUES
           |(1, '::1'),
           |(2, NULL),
           |(3, 'fe80::1')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_ipv6_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) != null)
    }
  }
  test("decode JSON - nullable with null values") {
    withKVTable("test_db", "test_json_null", valueColDef = "Nullable(String)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          """INSERT INTO $actualDb.test_json_null VALUES
            |(1, '{"name": "Alice", "age": 30}'),
            |(2, NULL),
            |(3, '{"name": "Charlie", "age": 35}')
            |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_json_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getString(1).contains("Alice"))
        assert(result(1).isNullAt(1))
        assert(result(2).getString(1).contains("Charlie"))
    }
  }
  test("decode JSON - semi-structured data") {
    withKVTable("test_db", "test_json", valueColDef = "String") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        """INSERT INTO $actualDb.test_json VALUES
          |(1, '{"name": "Alice", "age": 30}'),
          |(2, '{"name": "Bob", "age": 25}'),
          |(3, '{"name": "Charlie", "age": 35}')
          |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_json ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1).contains("Alice"))
      assert(result(1).getString(1).contains("Bob"))
      assert(result(2).getString(1).contains("Charlie"))
    }
  }
  test("decode LongType - min and max values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_long", valueColDef = "Int64") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_long VALUES
           |(1, -9223372036854775808),
           |(2, 0),
           |(3, 9223372036854775807)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_long ORDER BY key")
      checkAnswer(
        df,
        Row(1, -9223372036854775808L) :: Row(2, 0L) :: Row(3, 9223372036854775807L) :: Nil
      )
    }
  }
  test("decode LongType - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_long_null", valueColDef = "Nullable(Int64)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_long_null VALUES
           |(1, -9223372036854775808),
           |(2, NULL),
           |(3, 9223372036854775807)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_long_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -9223372036854775808L) :: Row(2, null) :: Row(3, 9223372036854775807L) :: Nil
      )
    }
  }
  test("decode LongType - UInt32 nullable with null values") {
    withKVTable("test_db", "test_uint32_null", valueColDef = "Nullable(UInt32)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_uint32_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 4294967295)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint32_null ORDER BY key")
        checkAnswer(
          df,
          Row(1, 0L) :: Row(2, null) :: Row(3, 4294967295L) :: Nil
        )
    }
  }
  test("decode LongType - UInt32 values") {
    withKVTable("test_db", "test_uint32", valueColDef = "UInt32") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uint32 VALUES
           |(1, 0),
           |(2, 2147483648),
           |(3, 4294967295)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint32 ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0L) :: Row(2, 2147483648L) :: Row(3, 4294967295L) :: Nil
      )
    }
  }
  test("decode MapType - Map of String to Int") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_map", valueColDef = "Map(String, Int32)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_map VALUES
           |(1, {'a': 1, 'b': 2}),
           |(2, {}),
           |(3, {'x': 100, 'y': 200, 'z': 300})
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_map ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getMap[String, Int](1) == Map("a" -> 1, "b" -> 2))
      assert(result(1).getMap[String, Int](1) == Map())
      assert(result(2).getMap[String, Int](1) == Map("x" -> 100, "y" -> 200, "z" -> 300))
    }
  }
  test("decode MapType - Map with nullable values") {
    withKVTable("test_db", "test_map_nullable", valueColDef = "Map(String, Nullable(Int32))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_map_nullable VALUES
             |(1, {'a': 1, 'b': NULL}),
             |(2, {'x': NULL}),
             |(3, {'p': 100, 'q': 200})
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_map_nullable ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        // Verify maps can be read
        assert(result(0).getMap[String, Any](1) != null)
        assert(result(1).getMap[String, Any](1) != null)
        assert(result(2).getMap[String, Any](1) != null)
    }
  }
  test("decode ShortType - min and max values") {
    withKVTable("test_db", "test_short", valueColDef = "Int16") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_short VALUES
           |(1, -32768),
           |(2, 0),
           |(3, 32767)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_short ORDER BY key")
      checkAnswer(
        df,
        Row(1, -32768.toShort) :: Row(2, 0.toShort) :: Row(3, 32767.toShort) :: Nil
      )
    }
  }
  test("decode ShortType - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_short_null", valueColDef = "Nullable(Int16)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_short_null VALUES
             |(1, -32768),
             |(2, NULL),
             |(3, 32767)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_short_null ORDER BY key")
        checkAnswer(
          df,
          Row(1, -32768.toShort) :: Row(2, null) :: Row(3, 32767.toShort) :: Nil
        )
    }
  }
  test("decode ShortType - UInt8 nullable with null values") {
    withKVTable("test_db", "test_uint8_null", valueColDef = "Nullable(UInt8)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_uint8_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 255)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint8_null ORDER BY key")
        checkAnswer(
          df,
          Row(1, 0.toShort) :: Row(2, null) :: Row(3, 255.toShort) :: Nil
        )
    }
  }
  test("decode ShortType - UInt8 values") {
    withKVTable("test_db", "test_uint8", valueColDef = "UInt8") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uint8 VALUES
           |(1, 0),
           |(2, 128),
           |(3, 255)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint8 ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0.toShort) :: Row(2, 128.toShort) :: Row(3, 255.toShort) :: Nil
      )
    }
  }
  test("decode StringType - empty strings") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_empty_string", valueColDef = "String") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_empty_string VALUES
           |(1, ''),
           |(2, 'not empty'),
           |(3, '')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_empty_string ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "")
      assert(result(1).getString(1) == "not empty")
      assert(result(2).getString(1) == "")
    }
  }
  test("decode StringType - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_string_null", valueColDef = "Nullable(String)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_string_null VALUES
             |(1, 'hello'),
             |(2, NULL),
             |(3, 'world')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_string_null ORDER BY key")
        checkAnswer(
          df,
          Row(1, "hello") :: Row(2, null) :: Row(3, "world") :: Nil
        )
    }
  }
  test("decode StringType - regular strings") {
    withKVTable("test_db", "test_string", valueColDef = "String") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_string VALUES
           |(1, 'hello'),
           |(2, ''),
           |(3, 'world with spaces'),
           |(4, 'special chars: !@#$$%^&*()')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_string ORDER BY key")
      checkAnswer(
        df,
        Row(1, "hello") :: Row(2, "") :: Row(3, "world with spaces") :: Row(4, "special chars: !@#$$%^&*()") :: Nil
      )
    }
  }
  test("decode StringType - UUID") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_uuid", valueColDef = "UUID") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uuid VALUES
           |(1, '550e8400-e29b-41d4-a716-446655440000'),
           |(2, '6ba7b810-9dad-11d1-80b4-00c04fd430c8')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uuid ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getString(1) == "550e8400-e29b-41d4-a716-446655440000")
      assert(result(1).getString(1) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    }
  }
  test("decode StringType - UUID nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_uuid_null", valueColDef = "Nullable(UUID)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uuid_null VALUES
           |(1, '550e8400-e29b-41d4-a716-446655440000'),
           |(2, NULL),
           |(3, '6ba7b810-9dad-11d1-80b4-00c04fd430c8')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uuid_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "550e8400-e29b-41d4-a716-446655440000")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    }
  }
  test("decode StringType - very long strings") {
    val longString = "a" * 10000
    withKVTable("test_db", "test_long_string", valueColDef = "String") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_long_string VALUES
           |(1, '$longString')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_long_string ORDER BY key")
      val result = df.collect()
      assert(result.length == 1)
      assert(result(0).getString(1).length == 10000)
    }
  }
  test("decode TimestampType - DateTime") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_datetime", valueColDef = "DateTime") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_datetime VALUES
           |(1, '2024-01-01 00:00:00'),
           |(2, '2024-06-15 12:30:45'),
           |(3, '2024-12-31 23:59:59')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_datetime ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode TimestampType - DateTime64") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_datetime64", valueColDef = "DateTime64(3)") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_datetime64 VALUES
           |(1, '2024-01-01 00:00:00.123'),
           |(2, '2024-06-15 12:30:45.456'),
           |(3, '2024-12-31 23:59:59.999')
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_datetime64 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode TimestampType - DateTime64 nullable with null values") {
    withKVTable("test_db", "test_datetime64_null", valueColDef = "Nullable(DateTime64(3))") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_datetime64_null VALUES
             |(1, '2024-01-01 00:00:00.123'),
             |(2, NULL),
             |(3, '2024-12-31 23:59:59.999')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_datetime64_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getTimestamp(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode TimestampType - nullable with null values") {
    withKVTable("test_db", "test_datetime_null", valueColDef = "Nullable(DateTime)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_datetime_null VALUES
             |(1, '2024-01-01 00:00:00'),
             |(2, NULL),
             |(3, '2024-12-31 23:59:59')
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_datetime_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getTimestamp(1) != null)
        assert(result(1).isNullAt(1))
        assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode UInt128 - large unsigned integers as Decimal") {
    withKVTable("test_db", "test_uint128", valueColDef = "UInt128") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uint128 VALUES
           |(1, 0),
           |(2, 123456789012345678901234567890)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint128 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
    }
  }
  test("decode UInt128 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_uint128_null", valueColDef = "Nullable(UInt128)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_uint128_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 123456789012345678901234567890)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint128_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode UInt16 - nullable with null values") {
    withKVTable("test_db", "test_uint16_null", valueColDef = "Nullable(UInt16)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_uint16_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 65535)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint16_null ORDER BY key")
        checkAnswer(
          df,
          Row(1, 0) :: Row(2, null) :: Row(3, 65535) :: Nil
        )
    }
  }
  test("decode UInt16 - unsigned 16-bit integers") {
    withKVTable("test_db", "test_uint16", valueColDef = "UInt16") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uint16 VALUES
           |(1, 0),
           |(2, 32768),
           |(3, 65535)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint16 ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0) :: Row(2, 32768) :: Row(3, 65535) :: Nil
      )
    }
  }
  test("decode UInt256 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_uint256_null", valueColDef = "Nullable(UInt256)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_uint256_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 12345678901234567890123456789012345678)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint256_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
        assert(result(1).isNullAt(1))
        assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode UInt256 - very large unsigned integers as Decimal") {
    withKVTable("test_db", "test_uint256", valueColDef = "UInt256") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uint256 VALUES
           |(1, 0),
           |(2, 12345678901234567890123456789012345678)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint256 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
    }
  }
  test("decode UInt64 - nullable with null values") { (actualDb: String, actualTbl: String) =>
    withKVTable("test_db", "test_uint64_null", valueColDef = "Nullable(UInt64)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.test_uint64_null VALUES
             |(1, 0),
             |(2, NULL),
             |(3, 9223372036854775807)
             |""".stripMargin
        )

        val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint64_null ORDER BY key")
        val result = df.collect()
        assert(result.length == 3)
        assert(result(0).getLong(1) == 0L)
        assert(result(1).isNullAt(1))
        assert(result(2).getLong(1) == 9223372036854775807L)
    }
  }
  test("decode UInt64 - unsigned 64-bit integers") {
    withKVTable("test_db", "test_uint64", valueColDef = "UInt64") { (actualDb: String, actualTbl: String) =>
      runClickHouseSQL(
        s"""INSERT INTO $actualDb.test_uint64 VALUES
           |(1, 0),
           |(2, 1234567890),
           |(3, 9223372036854775807)
           |""".stripMargin
      )

      val df = spark.sql(s"SELECT key, value FROM $actualDb.test_uint64 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getLong(1) == 0L)
      assert(result(1).getLong(1) == 1234567890L)
      // Max value that fits in signed Long
      assert(result(2).getLong(1) == 9223372036854775807L)
    }
  }

  // ============================================================================
  // StructType Tests
  // ============================================================================

  test("decode StructType - unnamed tuple created directly in ClickHouse") { (actualDb: String, actualTbl: String) =>
    val db = "test_db"
    val tbl = "test_read_unnamed_tuple"

    try {
      // Create database
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db")

      // Create table directly in ClickHouse with unnamed tuple
      runClickHouseSQL(
        s"""CREATE TABLE $db.$tbl (
           |  id Int64,
           |  data Tuple(String, Int32, String)
           |) ENGINE = MergeTree()
           |ORDER BY id
           |""".stripMargin
      )

      // Insert data directly via ClickHouse (unnamed tuple as array)
      runClickHouseSQL(
        s"""INSERT INTO $db.$tbl VALUES
           |  (1, ('Alice', 30, 'NYC')),
           |  (2, ('Bob', 25, 'LA')),
           |  (3, ('Charlie', 35, 'SF'))
           |""".stripMargin
      )

      // Read via Spark - should infer schema with field names _1, _2, _3
      val result = spark.table(s"$db.$tbl").sort("id").collect()

      assert(result.length === 3)

      // Verify first row
      val row0 = result(0)
      assert(row0.getLong(0) === 1L)
      val data0 = row0.getStruct(1)
      assert(data0.getString(0) === "Alice")
      assert(data0.getInt(1) === 30)
      assert(data0.getString(2) === "NYC")

      // Verify second row
      val row1 = result(1)
      assert(row1.getLong(0) === 2L)
      val data1 = row1.getStruct(1)
      assert(data1.getString(0) === "Bob")
      assert(data1.getInt(1) === 25)
      assert(data1.getString(2) === "LA")

      // Verify third row
      val row2 = result(2)
      assert(row2.getLong(0) === 3L)
      val data2 = row2.getStruct(1)
      assert(data2.getString(0) === "Charlie")
      assert(data2.getInt(1) === 35)
      assert(data2.getString(2) === "SF")

    } finally
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl")
  }

  // ============================================================================
  // VariantType Tests (ClickHouse 25.3+ JSON type)
  // ============================================================================

  // Helper to extract JSON string from VariantVal
  private def variantToJson(variantVal: org.apache.spark.unsafe.types.VariantVal): String = {
    val variant = new org.apache.spark.types.variant.Variant(variantVal.getValue, variantVal.getMetadata)
    variant.toJson(java.time.ZoneId.of("UTC"))
  }

  test("decode VariantType - simple JSON objects") {
    withKVTable("test_db", "test_variant_simple", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_simple VALUES
          |(1, '{"name": "Alice", "age": 30}'),
          |(2, '{"name": "Bob", "age": 25}'),
          |(3, '{"name": "Charlie", "age": 35}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_simple ORDER BY key")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect()
      assert(result.length == 3)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("Alice") && json1.contains("30"))

      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("Bob") && json2.contains("25"))

      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("Charlie") && json3.contains("35"))
    }
  }

  test("decode VariantType - nested JSON objects") {
    withKVTable("test_db", "test_variant_nested", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_nested VALUES
          |(1, '{"person": {"name": "Alice", "age": 30}, "city": "NYC"}'),
          |(2, '{"person": {"name": "Bob", "age": 25}, "city": "LA"}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_nested ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("Alice") && json1.contains("NYC"))

      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("Bob") && json2.contains("LA"))
    }
  }

  test("decode VariantType - JSON with arrays") {
    withKVTable("test_db", "test_variant_arrays", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_arrays VALUES
          |(1, '{"tags": ["a", "b", "c"], "nums": [1, 2, 3]}'),
          |(2, '{"tags": ["x", "y"], "nums": [10, 20]}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_arrays ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("tags") && json1.contains("nums"))
    }
  }

  test("decode VariantType - mixed types") {
    withKVTable("test_db", "test_variant_mixed", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_mixed VALUES
          |(1, '{"str": "text", "num": 42, "bool": true, "null": null}'),
          |(2, '{"str": "data", "num": 3.14, "bool": false}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_mixed ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("text") && json1.contains("42") && json1.contains("true"))
    }
  }

  test("decode VariantType - NULL values") {
    withKVTable("test_db", "test_variant_nulls", valueColDef = "Nullable(JSON)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_nulls VALUES
          |(1, '{"name": "Alice"}'),
          |(2, NULL),
          |(3, '{"name": "Charlie"}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_nulls ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(!result(0).isNullAt(1))
      assert(result(1).isNullAt(1))
      assert(!result(2).isNullAt(1))
    }
  }

  test("decode VariantType - empty JSON object") {
    withKVTable("test_db", "test_variant_empty", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_empty VALUES
          |(1, '{}'),
          |(2, '{"empty": []}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_empty ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("{}") || json1.trim == "{}")

      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("empty"))
    }
  }

  test("decode VariantType - numeric precision") {
    withKVTable("test_db", "test_variant_numbers", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_numbers VALUES
          |(1, '{"int": 42, "float": 3.14159, "large": 9223372036854775807}'),
          |(2, '{"negative": -123, "zero": 0, "decimal": 0.123456789}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_numbers ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("42") && json1.contains("3.14"))

      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("-123") && json2.contains("0.123"))
    }
  }

  test("decode VariantType - special characters") {
    withKVTable("test_db", "test_variant_special", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_special VALUES
          |(1, '{"text": "Hello World", "emoji": "🎉"}'),
          |(2, '{"unicode": "café", "symbol": "@#$%"}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_special ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(!result(0).isNullAt(1))
      assert(!result(1).isNullAt(1))
    }
  }

  test("decode VariantType - deeply nested structures") {
    withKVTable("test_db", "test_variant_deep", valueColDef = "JSON") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_deep VALUES
          |(1, '{"a": {"b": {"c": {"d": {"e": "deep"}}}}}'),
          |(2, '{"x": [{"y": [{"z": "nested"}]}]}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_deep ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)

      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("deep"))

      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("nested"))
    }
  }

  test("decode VariantType - read JSON as string with config") {
    withSQLConf("spark.clickhouse.read.jsonAs" -> "string") {
      withKVTable("test_db", "test_json_as_string", valueColDef = "JSON") {
        runClickHouseSQL(
          """INSERT INTO test_db.test_json_as_string VALUES
            |(1, '{"name": "Alice", "age": 30}'),
            |(2, '{"name": "Bob", "age": 25}')
            |""".stripMargin
        )

        val df = spark.sql("SELECT key, value FROM test_db.test_json_as_string ORDER BY key")
        val result = df.collect()

        // Verify schema is StringType, not VariantType
        assert(df.schema.fields(1).dataType == StringType)

        // Verify data is read as JSON strings
        assert(result.length == 2)
        val json1 = result(0).getString(1)
        assert(json1.contains("Alice") && json1.contains("30"))

        val json2 = result(1).getString(1)
        assert(json2.contains("Bob") && json2.contains("25"))
      }
    }
  }

  test("decode VariantType - mixed primitives and arrays") {
    withKVTable(
      "test_db",
      "test_variant_mixed_types",
      valueColDef = "Variant(String, Int64, Float64, Bool, Array(String))"
    ) {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_mixed_types VALUES
          |(1, 42),
          |(2, true),
          |(3, 'hello'),
          |(4, 3.14),
          |(5, ['a', 'b', 'c']),
          |(6, false)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_mixed_types")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect().sortBy(_.getInt(0))
      assert(result.length == 6)

      // Test primitive integer
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("42"))

      // Test primitive boolean (true)
      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("true"))

      // Test primitive string
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("hello"))

      // Test primitive float
      val json4 = variantToJson(result(3).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json4.contains("3.14"))

      // Test array
      val json5 = variantToJson(result(4).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json5.contains("a") && json5.contains("b") && json5.contains("c"))

      // Test primitive boolean (false)
      val json6 = variantToJson(result(5).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json6.contains("false"))
    }
  }

  test("decode VariantType - mixed primitives, arrays and JSON objects") {
    withKVTable(
      "test_db",
      "test_variant_with_json",
      valueColDef = "Variant(String, Int64, Float64, Bool, Array(String), JSON)"
    ) {
      runClickHouseSQL("SET allow_experimental_json_type = 1")
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_with_json VALUES
          |(1, 42),
          |(2, true),
          |(3, 'hello'),
          |(4, 3.14),
          |(5, ['a', 'b', 'c']),
          |(6, CAST('{"name": "Alice", "age": 30}' AS JSON)),
          |(7, CAST('{"city": "NYC", "country": "USA"}' AS JSON)),
          |(8, false)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_with_json")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect().sortBy(_.getInt(0))
      assert(result.length == 8)

      // Test primitive integer
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("42"))

      // Test primitive boolean (true)
      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("true"))

      // Test primitive string
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("hello"))

      // Test primitive float
      val json4 = variantToJson(result(3).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json4.contains("3.14"))

      // Test array
      val json5 = variantToJson(result(4).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json5.contains("a") && json5.contains("b") && json5.contains("c"))

      // Test JSON object 1
      val json6 = variantToJson(result(5).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json6.contains("Alice") && json6.contains("30"))

      // Test JSON object 2
      val json7 = variantToJson(result(6).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json7.contains("NYC") && json7.contains("USA"))

      // Test primitive boolean (false)
      val json8 = variantToJson(result(7).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json8.contains("false"))
    }
  }

  test("decode VariantType - explicit Variant with NULL values") {
    withKVTable(
      "test_db",
      "test_variant_nulls_explicit",
      valueColDef = "Nullable(Variant(String, Int64, Bool))"
    ) {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_nulls_explicit VALUES
          |(1, 'hello'),
          |(2, NULL),
          |(3, 42),
          |(4, NULL),
          |(5, true)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_nulls_explicit ORDER BY key")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect()
      assert(result.length == 5)

      // Row 1: string value
      assert(!result(0).isNullAt(1))
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("hello"))

      // Row 2: NULL
      assert(result(1).isNullAt(1))

      // Row 3: integer value
      assert(!result(2).isNullAt(1))
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("42"))

      // Row 4: NULL
      assert(result(3).isNullAt(1))

      // Row 5: boolean value
      assert(!result(4).isNullAt(1))
      val json5 = variantToJson(result(4).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json5.contains("true"))
    }
  }

  test("decode VariantType - Variant with empty arrays") {
    withKVTable(
      "test_db",
      "test_variant_empty_arrays",
      valueColDef = "Variant(String, Array(String), Array(Int64))"
    ) {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_empty_arrays VALUES
          |(1, []),
          |(2, 'text'),
          |(3, ['a', 'b']),
          |(4, [])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_empty_arrays ORDER BY key")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect()
      assert(result.length == 4)

      // Row 1: empty array
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("[]") || json1.trim == "[]")

      // Row 2: string
      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("text"))

      // Row 3: non-empty array
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("a") && json3.contains("b"))

      // Row 4: empty array
      val json4 = variantToJson(result(3).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json4.contains("[]") || json4.trim == "[]")
    }
  }

  test("decode VariantType - Variant with nested arrays") {
    withKVTable(
      "test_db",
      "test_variant_nested_arrays",
      valueColDef = "Variant(String, Array(Array(String)))"
    ) {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_nested_arrays VALUES
          |(1, [['a', 'b'], ['c', 'd']]),
          |(2, 'text'),
          |(3, [[]])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_nested_arrays ORDER BY key")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect()
      assert(result.length == 3)

      // Row 1: nested array
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("a") && json1.contains("b") && json1.contains("c") && json1.contains("d"))

      // Row 2: string
      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("text"))

      // Row 3: nested empty array
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("[]"))
    }
  }

  test("decode VariantType - Variant with only JSON type (default behavior)") {
    // When no explicit variant types are specified in Spark DDL, it defaults to JSON
    // JSON type only accepts JSON objects (starting with '{')
    withKVTable("test_db", "test_variant_json_only", valueColDef = "JSON") {
      runClickHouseSQL("SET allow_experimental_json_type = 1")
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_json_only VALUES
          |(1, '{"type": "object1", "value": 42}'),
          |(2, '{"type": "object2", "nested": {"key": "value"}}'),
          |(3, '{"type": "object3", "array": [1, 2, 3]}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_json_only ORDER BY key")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect()
      assert(result.length == 3)

      // Row 1: JSON object with number
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("object1") && json1.contains("42"))

      // Row 2: JSON object with nested object
      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("object2") && json2.contains("nested") && json2.contains("key"))

      // Row 3: JSON object with array
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("object3") && json3.contains("array"))
    }
  }

  test("decode VariantType - Variant with numeric types") {
    withKVTable(
      "test_db",
      "test_variant_numeric",
      valueColDef = "Variant(Int8, Int16, Int32, Int64, Float32, Float64)"
    ) {
      runClickHouseSQL(
        """INSERT INTO test_db.test_variant_numeric VALUES
          |(1, CAST(127 AS Int8)),
          |(2, CAST(32767 AS Int16)),
          |(3, CAST(2147483647 AS Int32)),
          |(4, CAST(9223372036854775807 AS Int64)),
          |(5, CAST(3.14 AS Float32)),
          |(6, CAST(2.718281828 AS Float64))
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_variant_numeric ORDER BY key")
      assert(df.schema.fields(1).dataType == VariantType)

      val result = df.collect()
      assert(result.length == 6)

      // Test various numeric types
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("127"))

      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("32767"))

      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("2147483647"))

      val json5 = variantToJson(result(4).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json5.contains("3.14"))
    }
  }

  test("end-to-end: Spark creates table with Variant, writes and reads data") {
    // Clean up any existing table
    spark.sql("DROP TABLE IF EXISTS clickhouse.test_db.test_e2e_variant")
    spark.sql("DROP DATABASE IF EXISTS clickhouse.test_db")
    spark.sql("CREATE DATABASE IF NOT EXISTS clickhouse.test_db")

    try {
      // Enable experimental JSON type for ClickHouse
      runClickHouseSQL("SET allow_experimental_json_type = 1")

      // Create table via Spark SQL with explicit Variant types
      spark.sql("""
        CREATE TABLE clickhouse.test_db.test_e2e_variant (
          id INT NOT NULL,
          simple_data VARIANT,
          complex_data VARIANT
        )
        USING clickhouse
        TBLPROPERTIES (
          'clickhouse.column.simple_data.variant_types' = 'String, Int64, Bool',
          'engine' = 'MergeTree()',
          'order_by' = 'id'
        )
      """)

      // Verify table was created
      val tables = spark.sql("SHOW TABLES IN clickhouse.test_db").collect()
      assert(tables.exists(_.getString(1) == "test_e2e_variant"), "Table should be created")

      // Write data using Spark SQL with parse_json
      import org.apache.spark.sql.functions._

      val testData = spark.createDataFrame(Seq(
        (1, """42""", """{"type":"object1","value":100}"""),
        (2, """"hello"""", """{"type":"object2","nested":{"key":"value"}}"""),
        (3, """true""", """{"type":"object3","array":[1,2,3]}"""),
        (4, """false""", """{"type":"object4","text":"data"}"""),
        (5, """12345""", """{"type":"object5","items":["a","b","c"]}""")
      )).toDF("id", "simple_json", "complex_json")

      val variantDF = testData
        .withColumn("simple_data", parse_json(col("simple_json")))
        .withColumn("complex_data", parse_json(col("complex_json")))
        .select("id", "simple_data", "complex_data")

      // Write to ClickHouse
      variantDF.writeTo("clickhouse.test_db.test_e2e_variant").append()

      // Read back the data
      val df = spark.sql("SELECT id, simple_data, complex_data FROM clickhouse.test_db.test_e2e_variant ORDER BY id")

      // Verify schema
      assert(df.schema.fields(1).dataType == VariantType, "simple_data should be VariantType")
      assert(df.schema.fields(2).dataType == VariantType, "complex_data should be VariantType")

      val result = df.collect()
      assert(result.length == 5, "Should have 5 rows")

      // Verify Row 1: integer and JSON object
      assert(result(0).getInt(0) == 1)
      val simple1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(simple1.contains("42"))
      val complex1 = variantToJson(result(0).get(2).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(complex1.contains("object1") && complex1.contains("100"))

      // Verify Row 2: string and nested JSON object
      assert(result(1).getInt(0) == 2)
      val simple2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(simple2.contains("hello"))
      val complex2 = variantToJson(result(1).get(2).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(complex2.contains("object2") && complex2.contains("nested") && complex2.contains("key"))

      // Verify Row 3: boolean and JSON with array
      assert(result(2).getInt(0) == 3)
      val simple3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(simple3.contains("true"))
      val complex3 = variantToJson(result(2).get(2).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(complex3.contains("object3") && complex3.contains("array"))

      // Verify Row 4: boolean false
      assert(result(3).getInt(0) == 4)
      val simple4 = variantToJson(result(3).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(simple4.contains("false"))

      // Verify Row 5: integer
      assert(result(4).getInt(0) == 5)
      val simple5 = variantToJson(result(4).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(simple5.contains("12345"))

      // Test filtering on Variant columns (read-only, no pushdown expected)
      val filtered = spark.sql("SELECT id FROM clickhouse.test_db.test_e2e_variant WHERE id > 2 ORDER BY id")
      val filteredResult = filtered.collect()
      assert(filteredResult.length == 3)
      assert(filteredResult(0).getInt(0) == 3)
      assert(filteredResult(1).getInt(0) == 4)
      assert(filteredResult(2).getInt(0) == 5)

    } finally {
      // Clean up
      spark.sql("DROP TABLE IF EXISTS clickhouse.test_db.test_e2e_variant")
      spark.sql("DROP DATABASE IF EXISTS clickhouse.test_db")
    }
  }

  test("end-to-end: Spark creates table with default JSON Variant, writes and reads data") {
    // Clean up any existing table
    spark.sql("DROP TABLE IF EXISTS clickhouse.test_db.test_e2e_json_default")
    spark.sql("DROP DATABASE IF EXISTS clickhouse.test_db")
    spark.sql("CREATE DATABASE IF NOT EXISTS clickhouse.test_db")

    try {
      // Enable experimental JSON type for ClickHouse
      runClickHouseSQL("SET allow_experimental_json_type = 1")

      // Create table via Spark SQL WITHOUT explicit variant types (defaults to JSON)
      spark.sql("""
        CREATE TABLE clickhouse.test_db.test_e2e_json_default (
          id INT NOT NULL,
          data VARIANT
        )
        USING clickhouse
        TBLPROPERTIES (
          'engine' = 'MergeTree()',
          'order_by' = 'id'
        )
      """)

      // Write data using Spark SQL - only JSON objects (JSON type restriction)
      import org.apache.spark.sql.functions._

      val testData = spark.createDataFrame(Seq(
        (1, """{"name":"Alice","age":30}"""),
        (2, """{"name":"Bob","age":25,"city":"NYC"}"""),
        (3, """{"nested":{"deep":"value"},"array":[1,2,3]}""")
      )).toDF("id", "data_json")

      val variantDF = testData
        .withColumn("data", parse_json(col("data_json")))
        .select("id", "data")

      // Write to ClickHouse
      variantDF.writeTo("clickhouse.test_db.test_e2e_json_default").append()

      // Read back the data
      val df = spark.sql("SELECT id, data FROM clickhouse.test_db.test_e2e_json_default ORDER BY id")

      // Verify schema
      assert(df.schema.fields(1).dataType == VariantType, "data should be VariantType")

      val result = df.collect()
      assert(result.length == 3, "Should have 3 rows")

      // Verify Row 1
      assert(result(0).getInt(0) == 1)
      val json1 = variantToJson(result(0).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json1.contains("Alice") && json1.contains("30"))

      // Verify Row 2
      assert(result(1).getInt(0) == 2)
      val json2 = variantToJson(result(1).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json2.contains("Bob") && json2.contains("NYC"))

      // Verify Row 3
      assert(result(2).getInt(0) == 3)
      val json3 = variantToJson(result(2).get(1).asInstanceOf[org.apache.spark.unsafe.types.VariantVal])
      assert(json3.contains("nested") && json3.contains("deep") && json3.contains("array"))

    } finally {
      // Clean up
      spark.sql("DROP TABLE IF EXISTS clickhouse.test_db.test_e2e_json_default")
      spark.sql("DROP DATABASE IF EXISTS clickhouse.test_db")
    }
  }

}
