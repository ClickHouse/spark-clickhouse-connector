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
    withKVTable("test_db", "test_array_int", valueColDef = "Array(Int32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_array_int VALUES
          |(1, [1, 2, 3]),
          |(2, []),
          |(3, [100, 200, 300, 400])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_array_int ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(1).getSeq[Int](1) == Seq())
      assert(result(2).getSeq[Int](1) == Seq(100, 200, 300, 400))
    }
  }
  test("decode ArrayType - Array of strings") {
    withKVTable("test_db", "test_array_string", valueColDef = "Array(String)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_array_string VALUES
          |(1, ['hello', 'world']),
          |(2, []),
          |(3, ['a', 'b', 'c'])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_array_string ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getSeq[String](1) == Seq("hello", "world"))
      assert(result(1).getSeq[String](1) == Seq())
      assert(result(2).getSeq[String](1) == Seq("a", "b", "c"))
    }
  }
  test("decode ArrayType - Array with nullable elements") {
    withKVTable("test_db", "test_array_nullable", valueColDef = "Array(Nullable(Int32))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_array_nullable VALUES
          |(1, [1, NULL, 3]),
          |(2, [NULL, NULL]),
          |(3, [100, 200])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_array_nullable ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      // Verify arrays can be read
      assert(result(0).getSeq[Any](1) != null)
      assert(result(1).getSeq[Any](1) != null)
      assert(result(2).getSeq[Any](1) != null)
    }
  }
  test("decode ArrayType - empty arrays") {
    withKVTable("test_db", "test_empty_array", valueColDef = "Array(Int32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_empty_array VALUES
          |(1, []),
          |(2, [1, 2, 3]),
          |(3, [])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_empty_array ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getSeq[Int](1).isEmpty)
      assert(result(1).getSeq[Int](1) == Seq(1, 2, 3))
      assert(result(2).getSeq[Int](1).isEmpty)
    }
  }
  test("decode ArrayType - Nested arrays") {
    withKVTable("test_db", "test_nested_array", valueColDef = "Array(Array(Int32))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_nested_array VALUES
          |(1, [[1, 2], [3, 4]]),
          |(2, [[], [5]]),
          |(3, [[10, 20, 30]])
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_nested_array ORDER BY key")
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
      runClickHouseSQL(
        """INSERT INTO test_db.test_fixedstring VALUES
          |(1, 'hello'),
          |(2, 'world')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_fixedstring ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      // FixedString should be readable
      assert(result(0).get(1) != null)
      assert(result(1).get(1) != null)
    }
  }
  test("decode BinaryType - FixedString nullable with null values") {
    withKVTable("test_db", "test_fixedstring_null", valueColDef = "Nullable(FixedString(5))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_fixedstring_null VALUES
          |(1, 'hello'),
          |(2, NULL),
          |(3, 'world')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_fixedstring_null ORDER BY key")
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
    withKVTable("test_db", "test_bool", valueColDef = "Bool") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_bool VALUES
          |(1, true),
          |(2, false),
          |(3, 1),
          |(4, 0)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_bool ORDER BY key")
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
  test("decode BooleanType - nullable with null values") {
    withKVTable("test_db", "test_bool_null", valueColDef = "Nullable(Bool)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_bool_null VALUES
          |(1, true),
          |(2, NULL),
          |(3, false)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_bool_null ORDER BY key")
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
    withKVTable("test_db", "test_byte", valueColDef = "Int8") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_byte VALUES
          |(1, -128),
          |(2, 0),
          |(3, 127)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_byte ORDER BY key")
      checkAnswer(
        df,
        Row(1, -128.toByte) :: Row(2, 0.toByte) :: Row(3, 127.toByte) :: Nil
      )
    }
  }
  test("decode ByteType - nullable with null values") {
    withKVTable("test_db", "test_byte_null", valueColDef = "Nullable(Int8)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_byte_null VALUES
          |(1, -128),
          |(2, NULL),
          |(3, 127)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_byte_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -128.toByte) :: Row(2, null) :: Row(3, 127.toByte) :: Nil
      )
    }
  }
  test("decode DateTime32 - 32-bit timestamp") {
    withKVTable("test_db", "test_datetime32", valueColDef = "DateTime32") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_datetime32 VALUES
          |(1, '2024-01-01 12:00:00'),
          |(2, '2024-06-15 18:30:45')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_datetime32 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
    }
  }
  test("decode DateTime32 - nullable with null values") {
    withKVTable("test_db", "test_datetime32_null", valueColDef = "Nullable(DateTime32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_datetime32_null VALUES
          |(1, '2024-01-01 12:00:00'),
          |(2, NULL),
          |(3, '2024-06-15 18:30:45')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_datetime32_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode DateType - Date") {
    withKVTable("test_db", "test_date", valueColDef = "Date") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_date VALUES
          |(1, '2024-01-01'),
          |(2, '2024-06-15'),
          |(3, '2024-12-31')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_date ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).getDate(1) != null)
      assert(result(2).getDate(1) != null)
    }
  }
  test("decode DateType - Date32") {
    withKVTable("test_db", "test_date32", valueColDef = "Date32") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_date32 VALUES
          |(1, '1900-01-01'),
          |(2, '2024-06-15'),
          |(3, '2100-12-31')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_date32 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).getDate(1) != null)
      assert(result(2).getDate(1) != null)
    }
  }
  test("decode DateType - Date32 nullable with null values") {
    withKVTable("test_db", "test_date32_null", valueColDef = "Nullable(Date32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_date32_null VALUES
          |(1, '1900-01-01'),
          |(2, NULL),
          |(3, '2100-12-31')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_date32_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDate(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getDate(1) != null)
    }
  }
  test("decode DateType - nullable with null values") {
    withKVTable("test_db", "test_date_null", valueColDef = "Nullable(Date)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_date_null VALUES
          |(1, '2024-01-01'),
          |(2, NULL),
          |(3, '2024-12-31')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_date_null ORDER BY key")
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
    withKVTable("test_db", "test_decimal128", valueColDef = "Decimal128(20)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_decimal128 VALUES
          |(1, 123456789012345.12345678901234567890),
          |(2, -999999999999999.99999999999999999999),
          |(3, 0.00000000000000000001)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_decimal128 ORDER BY key")
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
      runClickHouseSQL(
        """INSERT INTO test_db.test_decimal128_null VALUES
          |(1, 123456789012345.12345678901234567890),
          |(2, NULL),
          |(3, 0.00000000000000000001)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_decimal128_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode DecimalType - Decimal32") {
    withKVTable("test_db", "test_decimal32", valueColDef = "Decimal32(4)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_decimal32 VALUES
          |(1, 12345.6789),
          |(2, -9999.9999),
          |(3, 0.0001)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_decimal32 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).doubleValue() == 12345.6789)
      assert(result(1).getDecimal(1).doubleValue() == -9999.9999)
      assert(result(2).getDecimal(1).doubleValue() == 0.0001)
    }
  }
  test("decode DecimalType - Decimal32 nullable with null values") {
    withKVTable("test_db", "test_decimal32_null", valueColDef = "Nullable(Decimal32(4))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_decimal32_null VALUES
          |(1, 12345.6789),
          |(2, NULL),
          |(3, 0.0001)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_decimal32_null ORDER BY key")
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
    withKVTable("test_db", "test_decimal64", valueColDef = "Decimal64(10)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_decimal64 VALUES
          |(1, 1234567.0123456789),
          |(2, -9999999.9999999999),
          |(3, 0.0000000001)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_decimal64 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDecimal(1).doubleValue() - 1234567.0123456789) < 0.0001)
      assert(math.abs(result(1).getDecimal(1).doubleValue() - -9999999.9999999999) < 0.0001)
      assert(math.abs(result(2).getDecimal(1).doubleValue() - 0.0000000001) < 0.0000000001)
    }
  }
  test("decode DecimalType - Decimal64 nullable with null values") {
    withKVTable("test_db", "test_decimal64_null", valueColDef = "Nullable(Decimal64(10))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_decimal64_null VALUES
          |(1, 1234567.0123456789),
          |(2, NULL),
          |(3, 0.0000000001)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_decimal64_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode DoubleType - nullable with null values") {
    withKVTable("test_db", "test_double_null", valueColDef = "Nullable(Float64)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_double_null VALUES
          |(1, 1.23),
          |(2, NULL),
          |(3, -4.56)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_double_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDouble(1) - 1.23) < 0.0001)
      assert(result(1).isNullAt(1))
      assert(math.abs(result(2).getDouble(1) - -4.56) < 0.0001)
    }
  }
  test("decode DoubleType - regular values") {
    withKVTable("test_db", "test_double", valueColDef = "Float64") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_double VALUES
          |(1, -3.141592653589793),
          |(2, 0.0),
          |(3, 3.141592653589793)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_double ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getDouble(1) - -3.141592653589793) < 0.000001)
      assert(result(1).getDouble(1) == 0.0)
      assert(math.abs(result(2).getDouble(1) - 3.141592653589793) < 0.000001)
    }
  }
  test("decode Enum16 - large enum") {
    withKVTable("test_db", "test_enum16", valueColDef = "Enum16('small' = 1, 'medium' = 100, 'large' = 1000)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_enum16 VALUES
          |(1, 'small'),
          |(2, 'medium'),
          |(3, 'large')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_enum16 ORDER BY key")
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
    ) {
      runClickHouseSQL(
        """INSERT INTO test_db.test_enum16_null VALUES
          |(1, 'small'),
          |(2, NULL),
          |(3, 'large')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_enum16_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "small")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "large")
    }
  }
  test("decode Enum8 - nullable with null values") {
    withKVTable("test_db", "test_enum8_null", valueColDef = "Nullable(Enum8('red' = 1, 'green' = 2, 'blue' = 3))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_enum8_null VALUES
          |(1, 'red'),
          |(2, NULL),
          |(3, 'blue')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_enum8_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "red")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "blue")
    }
  }
  test("decode Enum8 - small enum") {
    withKVTable("test_db", "test_enum8", valueColDef = "Enum8('red' = 1, 'green' = 2, 'blue' = 3)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_enum8 VALUES
          |(1, 'red'),
          |(2, 'green'),
          |(3, 'blue')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_enum8 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "red")
      assert(result(1).getString(1) == "green")
      assert(result(2).getString(1) == "blue")
    }
  }
  test("decode FloatType - nullable with null values") {
    withKVTable("test_db", "test_float_null", valueColDef = "Nullable(Float32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_float_null VALUES
          |(1, 1.5),
          |(2, NULL),
          |(3, -2.5)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_float_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getFloat(1) - 1.5f) < 0.01f)
      assert(result(1).isNullAt(1))
      assert(math.abs(result(2).getFloat(1) - -2.5f) < 0.01f)
    }
  }
  test("decode FloatType - regular values") {
    withKVTable("test_db", "test_float", valueColDef = "Float32") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_float VALUES
          |(1, -3.14),
          |(2, 0.0),
          |(3, 3.14)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_float ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(math.abs(result(0).getFloat(1) - -3.14f) < 0.01f)
      assert(result(1).getFloat(1) == 0.0f)
      assert(math.abs(result(2).getFloat(1) - 3.14f) < 0.01f)
    }
  }
  test("decode Int128 - large integers as Decimal") {
    withKVTable("test_db", "test_int128", valueColDef = "Int128") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_int128 VALUES
          |(1, 0),
          |(2, 123456789012345678901234567890),
          |(3, -123456789012345678901234567890)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_int128 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode Int128 - nullable with null values") {
    withKVTable("test_db", "test_int128_null", valueColDef = "Nullable(Int128)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_int128_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, -123456789012345678901234567890)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_int128_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode Int256 - nullable with null values") {
    withKVTable("test_db", "test_int256_null", valueColDef = "Nullable(Int256)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_int256_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 12345678901234567890123456789012345678)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_int256_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode Int256 - very large integers as Decimal") {
    withKVTable("test_db", "test_int256", valueColDef = "Int256") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_int256 VALUES
          |(1, 0),
          |(2, 12345678901234567890123456789012345678)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_int256 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
    }
  }
  test("decode IntegerType - min and max values") {
    withKVTable("test_db", "test_int", valueColDef = "Int32") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_int VALUES
          |(1, -2147483648),
          |(2, 0),
          |(3, 2147483647)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_int ORDER BY key")
      checkAnswer(
        df,
        Row(1, -2147483648) :: Row(2, 0) :: Row(3, 2147483647) :: Nil
      )
    }
  }
  test("decode IntegerType - nullable with null values") {
    withKVTable("test_db", "test_int_null", valueColDef = "Nullable(Int32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_int_null VALUES
          |(1, -2147483648),
          |(2, NULL),
          |(3, 2147483647)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_int_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -2147483648) :: Row(2, null) :: Row(3, 2147483647) :: Nil
      )
    }
  }
  test("decode IPv4 - IP addresses") {
    withKVTable("test_db", "test_ipv4", valueColDef = "IPv4") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_ipv4 VALUES
          |(1, '127.0.0.1'),
          |(2, '192.168.1.1'),
          |(3, '8.8.8.8')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_ipv4 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "127.0.0.1")
      assert(result(1).getString(1) == "192.168.1.1")
      assert(result(2).getString(1) == "8.8.8.8")
    }
  }
  test("decode IPv4 - nullable with null values") {
    withKVTable("test_db", "test_ipv4_null", valueColDef = "Nullable(IPv4)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_ipv4_null VALUES
          |(1, '127.0.0.1'),
          |(2, NULL),
          |(3, '8.8.8.8')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_ipv4_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "127.0.0.1")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "8.8.8.8")
    }
  }
  test("decode IPv6 - IPv6 addresses") {
    withKVTable("test_db", "test_ipv6", valueColDef = "IPv6") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_ipv6 VALUES
          |(1, '::1'),
          |(2, '2001:0db8:85a3:0000:0000:8a2e:0370:7334'),
          |(3, 'fe80::1')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_ipv6 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) != null)
      assert(result(1).getString(1) != null)
      assert(result(2).getString(1) != null)
    }
  }
  test("decode IPv6 - nullable with null values") {
    withKVTable("test_db", "test_ipv6_null", valueColDef = "Nullable(IPv6)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_ipv6_null VALUES
          |(1, '::1'),
          |(2, NULL),
          |(3, 'fe80::1')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_ipv6_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) != null)
    }
  }
  test("decode JSON - nullable with null values") {
    withKVTable("test_db", "test_json_null", valueColDef = "Nullable(String)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_json_null VALUES
          |(1, '{"name": "Alice", "age": 30}'),
          |(2, NULL),
          |(3, '{"name": "Charlie", "age": 35}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_json_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1).contains("Alice"))
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1).contains("Charlie"))
    }
  }
  test("decode JSON - semi-structured data") {
    withKVTable("test_db", "test_json", valueColDef = "String") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_json VALUES
          |(1, '{"name": "Alice", "age": 30}'),
          |(2, '{"name": "Bob", "age": 25}'),
          |(3, '{"name": "Charlie", "age": 35}')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_json ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1).contains("Alice"))
      assert(result(1).getString(1).contains("Bob"))
      assert(result(2).getString(1).contains("Charlie"))
    }
  }
  test("decode LongType - min and max values") {
    withKVTable("test_db", "test_long", valueColDef = "Int64") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_long VALUES
          |(1, -9223372036854775808),
          |(2, 0),
          |(3, 9223372036854775807)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_long ORDER BY key")
      checkAnswer(
        df,
        Row(1, -9223372036854775808L) :: Row(2, 0L) :: Row(3, 9223372036854775807L) :: Nil
      )
    }
  }
  test("decode LongType - nullable with null values") {
    withKVTable("test_db", "test_long_null", valueColDef = "Nullable(Int64)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_long_null VALUES
          |(1, -9223372036854775808),
          |(2, NULL),
          |(3, 9223372036854775807)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_long_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -9223372036854775808L) :: Row(2, null) :: Row(3, 9223372036854775807L) :: Nil
      )
    }
  }
  test("decode LongType - UInt32 nullable with null values") {
    withKVTable("test_db", "test_uint32_null", valueColDef = "Nullable(UInt32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint32_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 4294967295)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint32_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0L) :: Row(2, null) :: Row(3, 4294967295L) :: Nil
      )
    }
  }
  test("decode LongType - UInt32 values") {
    withKVTable("test_db", "test_uint32", valueColDef = "UInt32") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint32 VALUES
          |(1, 0),
          |(2, 2147483648),
          |(3, 4294967295)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint32 ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0L) :: Row(2, 2147483648L) :: Row(3, 4294967295L) :: Nil
      )
    }
  }
  test("decode MapType - Map of String to Int") {
    withKVTable("test_db", "test_map", valueColDef = "Map(String, Int32)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_map VALUES
          |(1, {'a': 1, 'b': 2}),
          |(2, {}),
          |(3, {'x': 100, 'y': 200, 'z': 300})
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_map ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getMap[String, Int](1) == Map("a" -> 1, "b" -> 2))
      assert(result(1).getMap[String, Int](1) == Map())
      assert(result(2).getMap[String, Int](1) == Map("x" -> 100, "y" -> 200, "z" -> 300))
    }
  }
  test("decode MapType - Map with nullable values") {
    withKVTable("test_db", "test_map_nullable", valueColDef = "Map(String, Nullable(Int32))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_map_nullable VALUES
          |(1, {'a': 1, 'b': NULL}),
          |(2, {'x': NULL}),
          |(3, {'p': 100, 'q': 200})
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_map_nullable ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      // Verify maps can be read
      assert(result(0).getMap[String, Any](1) != null)
      assert(result(1).getMap[String, Any](1) != null)
      assert(result(2).getMap[String, Any](1) != null)
    }
  }
  test("decode ShortType - min and max values") {
    withKVTable("test_db", "test_short", valueColDef = "Int16") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_short VALUES
          |(1, -32768),
          |(2, 0),
          |(3, 32767)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_short ORDER BY key")
      checkAnswer(
        df,
        Row(1, -32768.toShort) :: Row(2, 0.toShort) :: Row(3, 32767.toShort) :: Nil
      )
    }
  }
  test("decode ShortType - nullable with null values") {
    withKVTable("test_db", "test_short_null", valueColDef = "Nullable(Int16)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_short_null VALUES
          |(1, -32768),
          |(2, NULL),
          |(3, 32767)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_short_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, -32768.toShort) :: Row(2, null) :: Row(3, 32767.toShort) :: Nil
      )
    }
  }
  test("decode ShortType - UInt8 nullable with null values") {
    withKVTable("test_db", "test_uint8_null", valueColDef = "Nullable(UInt8)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint8_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 255)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint8_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0.toShort) :: Row(2, null) :: Row(3, 255.toShort) :: Nil
      )
    }
  }
  test("decode ShortType - UInt8 values") {
    withKVTable("test_db", "test_uint8", valueColDef = "UInt8") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint8 VALUES
          |(1, 0),
          |(2, 128),
          |(3, 255)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint8 ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0.toShort) :: Row(2, 128.toShort) :: Row(3, 255.toShort) :: Nil
      )
    }
  }
  test("decode StringType - empty strings") {
    withKVTable("test_db", "test_empty_string", valueColDef = "String") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_empty_string VALUES
          |(1, ''),
          |(2, 'not empty'),
          |(3, '')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_empty_string ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "")
      assert(result(1).getString(1) == "not empty")
      assert(result(2).getString(1) == "")
    }
  }
  test("decode StringType - nullable with null values") {
    withKVTable("test_db", "test_string_null", valueColDef = "Nullable(String)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_string_null VALUES
          |(1, 'hello'),
          |(2, NULL),
          |(3, 'world')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_string_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, "hello") :: Row(2, null) :: Row(3, "world") :: Nil
      )
    }
  }
  test("decode StringType - regular strings") {
    withKVTable("test_db", "test_string", valueColDef = "String") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_string VALUES
          |(1, 'hello'),
          |(2, ''),
          |(3, 'world with spaces'),
          |(4, 'special chars: !@#$%^&*()')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_string ORDER BY key")
      checkAnswer(
        df,
        Row(1, "hello") :: Row(2, "") :: Row(3, "world with spaces") :: Row(4, "special chars: !@#$%^&*()") :: Nil
      )
    }
  }
  test("decode StringType - UUID") {
    withKVTable("test_db", "test_uuid", valueColDef = "UUID") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uuid VALUES
          |(1, '550e8400-e29b-41d4-a716-446655440000'),
          |(2, '6ba7b810-9dad-11d1-80b4-00c04fd430c8')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uuid ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getString(1) == "550e8400-e29b-41d4-a716-446655440000")
      assert(result(1).getString(1) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    }
  }
  test("decode StringType - UUID nullable with null values") {
    withKVTable("test_db", "test_uuid_null", valueColDef = "Nullable(UUID)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uuid_null VALUES
          |(1, '550e8400-e29b-41d4-a716-446655440000'),
          |(2, NULL),
          |(3, '6ba7b810-9dad-11d1-80b4-00c04fd430c8')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uuid_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "550e8400-e29b-41d4-a716-446655440000")
      assert(result(1).isNullAt(1))
      assert(result(2).getString(1) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    }
  }
  test("decode StringType - very long strings") {
    val longString = "a" * 10000
    withKVTable("test_db", "test_long_string", valueColDef = "String") {
      runClickHouseSQL(
        s"""INSERT INTO test_db.test_long_string VALUES
           |(1, '$longString')
           |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_long_string ORDER BY key")
      val result = df.collect()
      assert(result.length == 1)
      assert(result(0).getString(1).length == 10000)
    }
  }
  test("decode TimestampType - DateTime") {
    withKVTable("test_db", "test_datetime", valueColDef = "DateTime") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_datetime VALUES
          |(1, '2024-01-01 00:00:00'),
          |(2, '2024-06-15 12:30:45'),
          |(3, '2024-12-31 23:59:59')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_datetime ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode TimestampType - DateTime64") {
    withKVTable("test_db", "test_datetime64", valueColDef = "DateTime64(3)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_datetime64 VALUES
          |(1, '2024-01-01 00:00:00.123'),
          |(2, '2024-06-15 12:30:45.456'),
          |(3, '2024-12-31 23:59:59.999')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_datetime64 ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).getTimestamp(1) != null)
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode TimestampType - DateTime64 nullable with null values") {
    withKVTable("test_db", "test_datetime64_null", valueColDef = "Nullable(DateTime64(3))") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_datetime64_null VALUES
          |(1, '2024-01-01 00:00:00.123'),
          |(2, NULL),
          |(3, '2024-12-31 23:59:59.999')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_datetime64_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode TimestampType - nullable with null values") {
    withKVTable("test_db", "test_datetime_null", valueColDef = "Nullable(DateTime)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_datetime_null VALUES
          |(1, '2024-01-01 00:00:00'),
          |(2, NULL),
          |(3, '2024-12-31 23:59:59')
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_datetime_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getTimestamp(1) != null)
      assert(result(1).isNullAt(1))
      assert(result(2).getTimestamp(1) != null)
    }
  }
  test("decode UInt128 - large unsigned integers as Decimal") {
    withKVTable("test_db", "test_uint128", valueColDef = "UInt128") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint128 VALUES
          |(1, 0),
          |(2, 123456789012345678901234567890)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint128 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
    }
  }
  test("decode UInt128 - nullable with null values") {
    withKVTable("test_db", "test_uint128_null", valueColDef = "Nullable(UInt128)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint128_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 123456789012345678901234567890)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint128_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode UInt16 - nullable with null values") {
    withKVTable("test_db", "test_uint16_null", valueColDef = "Nullable(UInt16)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint16_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 65535)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint16_null ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0) :: Row(2, null) :: Row(3, 65535) :: Nil
      )
    }
  }
  test("decode UInt16 - unsigned 16-bit integers") {
    withKVTable("test_db", "test_uint16", valueColDef = "UInt16") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint16 VALUES
          |(1, 0),
          |(2, 32768),
          |(3, 65535)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint16 ORDER BY key")
      checkAnswer(
        df,
        Row(1, 0) :: Row(2, 32768) :: Row(3, 65535) :: Nil
      )
    }
  }
  test("decode UInt256 - nullable with null values") {
    withKVTable("test_db", "test_uint256_null", valueColDef = "Nullable(UInt256)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint256_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 12345678901234567890123456789012345678)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint256_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).isNullAt(1))
      assert(result(2).getDecimal(1) != null)
    }
  }
  test("decode UInt256 - very large unsigned integers as Decimal") {
    withKVTable("test_db", "test_uint256", valueColDef = "UInt256") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint256 VALUES
          |(1, 0),
          |(2, 12345678901234567890123456789012345678)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint256 ORDER BY key")
      val result = df.collect()
      assert(result.length == 2)
      assert(result(0).getDecimal(1).toBigInteger.longValue == 0L)
      assert(result(1).getDecimal(1) != null)
    }
  }
  test("decode UInt64 - nullable with null values") {
    withKVTable("test_db", "test_uint64_null", valueColDef = "Nullable(UInt64)") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint64_null VALUES
          |(1, 0),
          |(2, NULL),
          |(3, 9223372036854775807)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint64_null ORDER BY key")
      val result = df.collect()
      assert(result.length == 3)
      assert(result(0).getLong(1) == 0L)
      assert(result(1).isNullAt(1))
      assert(result(2).getLong(1) == 9223372036854775807L)
    }
  }
  test("decode UInt64 - unsigned 64-bit integers") {
    withKVTable("test_db", "test_uint64", valueColDef = "UInt64") {
      runClickHouseSQL(
        """INSERT INTO test_db.test_uint64 VALUES
          |(1, 0),
          |(2, 1234567890),
          |(3, 9223372036854775807)
          |""".stripMargin
      )

      val df = spark.sql("SELECT key, value FROM test_db.test_uint64 ORDER BY key")
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

  test("decode StructType - unnamed tuple created directly in ClickHouse") {
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

}
