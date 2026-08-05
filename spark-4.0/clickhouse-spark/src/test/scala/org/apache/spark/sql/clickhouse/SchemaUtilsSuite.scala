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

package org.apache.spark.sql.clickhouse

import com.clickhouse.data.ClickHouseColumn
import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.sql.clickhouse.SchemaUtils._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class SchemaUtilsSuite extends AnyFunSuite {

  case class TestBean(chTypeStr: String, sparkType: DataType, nullable: Boolean)

  private def assertPositive(positives: TestBean*): Unit =
    positives.foreach { case TestBean(chTypeStr, expectedSparkType, expectedNullable) =>
      test(s"ch2spark - $chTypeStr") {
        val chCols = ClickHouseColumn.parse(s"`col` $chTypeStr")
        assert(chCols.size == 1)
        val (actualSparkType, actualNullable) = fromClickHouseType(chCols.get(0)).get
        assert(actualSparkType === expectedSparkType)
        assert(actualNullable === expectedNullable)
      }
    }

  private def assertNegative(negatives: String*): Unit = negatives.foreach { chTypeStr =>
    test(s"ch2spark - $chTypeStr") {
      intercept[Exception] {
        ClickHouseColumn.parse(s"`col` $chTypeStr")
        val chCols = ClickHouseColumn.parse(s"`col` $chTypeStr")
        assert(chCols.size == 1)
        fromClickHouseType(chCols.get(0))
      }
    }
  }

  private def assertUnsupported(unsupported: String*): Unit = unsupported.foreach { chTypeStr =>
    test(s"ch2spark - $chTypeStr") {
      val chCols = ClickHouseColumn.parse(s"`col` $chTypeStr")
      assert(chCols.size == 1)
      assert(fromClickHouseType(chCols.get(0)).isEmpty)
    }
  }

  assertPositive(
    TestBean(
      "Array(String)",
      ArrayType(StringType, containsNull = false),
      nullable = false
    ),
    TestBean(
      "Array(Nullable(String))",
      ArrayType(StringType, containsNull = true),
      nullable = false
    ),
    TestBean(
      "Array(Array(String))",
      ArrayType(ArrayType(StringType, containsNull = false), containsNull = false),
      nullable = false
    )
  )

  assertNegative(
    "array(String)",
    "Array(String"
  )

  assertPositive(
    TestBean(
      "Map(String, String)",
      MapType(StringType, StringType, valueContainsNull = false),
      nullable = false
    ),
    TestBean(
      "Map(String,Int32)",
      MapType(StringType, IntegerType, valueContainsNull = false),
      nullable = false
    ),
    TestBean(
      "Map(String,Nullable(UInt32))",
      MapType(StringType, LongType, valueContainsNull = true),
      nullable = false
    )
  )

  assertNegative(
    "Map(String,)"
  )

  assertPositive(
    TestBean(
      "Date",
      DateType,
      nullable = false
    ),
    TestBean(
      "DateTime",
      TimestampType,
      nullable = false
    ),
    TestBean(
      "DateTime(Asia/Shanghai)",
      TimestampType,
      nullable = false
    ),
    TestBean(
      "DateTime64",
      TimestampType,
      nullable = false
    )
    // TestBean(
    //   "DateTime64(Europe/Moscow)",
    //   TimestampType,
    //   nullable = false
    // ),
  )

  assertNegative(
    "DT"
  )

  assertPositive(
    TestBean(
      "Decimal(2,1)",
      DecimalType(2, 1),
      nullable = false
    ),
    TestBean(
      "Decimal32(5)",
      DecimalType(9, 5),
      nullable = false
    ),
    TestBean(
      "Decimal64(5)",
      DecimalType(18, 5),
      nullable = false
    ),
    TestBean(
      "Decimal128(5)",
      DecimalType(38, 5),
      nullable = false
    )
  )

  assertNegative(
    "Decimal", // overflow
    "Decimal(String"
    // "Decimal32(5"
  )

  assertPositive(
    TestBean(
      "String",
      StringType,
      nullable = false
    ),
    TestBean(
      "FixedString(5)",
      BinaryType,
      nullable = false
    ),
    TestBean(
      "LowCardinality(String)",
      StringType,
      nullable = false
    ),
    TestBean(
      "LowCardinality(FixedString(5))",
      BinaryType,
      nullable = false
    ),
    TestBean(
      "LowCardinality(Int32)", // illegal actually
      IntegerType,
      nullable = false
    )
  )

  assertNegative("fixedString(5)")

  assertUnsupported(
    "AggregateFunction(sum, Int32)",
    "SimpleAggregateFunction(sum, Int64)",
    "Decimal256(5)",
    "Array(AggregateFunction(sum, Int32))",
    "Map(String, AggregateFunction(sum, Int32))",
    "Tuple(Int32, AggregateFunction(sum, Int32))"
  )

  test("spark2ch") {
    val catalystSchema = StructType.fromString(
      """{
        |  "type": "struct",
        |  "fields": [
        |    {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
        |    {"name": "food", "type": "string", "nullable": false, "metadata": {"comment": "food"}},
        |    {"name": "price", "type": "decimal(2,1)", "nullable": false, "metadata": {"comment": "price usd"}},
        |    {"name": "remark", "type": "string", "nullable": true, "metadata": {}},
        |    {"name": "ingredient", "type": {"type": "array", "elementType": "string", "containsNull": true}, "nullable": true, "metadata": {}},
        |    {"name": "nutrient", "type": {"type": "map", "keyType": "string", "valueType": "string", "valueContainsNull": true}, "nullable": true, "metadata": {}}
        |  ]
        |}
        |""".stripMargin
    )
    assert(Seq(
      ("id", "Int32", ""),
      ("food", "String", " COMMENT 'food'"),
      ("price", "Decimal(2, 1)", " COMMENT 'price usd'"),
      ("remark", "Nullable(String)", ""),
      ("ingredient", "Array(Nullable(String))", ""),
      ("nutrient", "Map(String, Nullable(String))", "")
    ) == toClickHouseSchema(catalystSchema))
  }

  private val mixedChSchema = Seq(
    "customer_id" -> "Int32",
    "agg_state" -> "AggregateFunction(sum, Int32)",
    "avg_state" -> "AggregateFunction(avg, Float64)",
    "client_id" -> "String"
  )

  test("fromClickHouseSchema maps columns with unsupported types to the placeholder type") {
    val actual = fromClickHouseSchema(mixedChSchema)
    val expected = StructType(
      StructField("customer_id", IntegerType, nullable = false) ::
        ClickHouseUnsupportedType.field("agg_state", "AggregateFunction(sum, Int32)") ::
        ClickHouseUnsupportedType.field("avg_state", "AggregateFunction(avg, Float64)") ::
        StructField("client_id", StringType, nullable = false) :: Nil
    )
    assert(actual === expected)
    assert(ClickHouseUnsupportedType.unsupportedColumns(actual) === Seq(
      "agg_state" -> "AggregateFunction(sum, Int32)",
      "avg_state" -> "AggregateFunction(avg, Float64)"
    ))
  }

  test("fromClickHouseSchema keeps columns the client can not parse as the placeholder type") {
    val actual = fromClickHouseSchema(Seq("c" -> "SomeFutureType(42)"))
    assert(actual === StructType(ClickHouseUnsupportedType.field("c", "SomeFutureType(42)") :: Nil))
    assert(ClickHouseUnsupportedType.unsupportedColumns(actual) === Seq("c" -> "SomeFutureType(42)"))
  }

  test("validateCreateSchema rejects schemas with placeholder columns") {
    val e = intercept[Exception](toClickHouseSchema(fromClickHouseSchema(mixedChSchema)))
    assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
    assert(e.getMessage.contains("`avg_state` AggregateFunction(avg, Float64)"))
    // a fully supported schema passes
    assert(toClickHouseSchema(fromClickHouseSchema(Seq("id" -> "Int32"))).nonEmpty)
  }

  test("placeholder type serializes to schema JSON as void so non-JVM clients can parse it") {
    val schema = fromClickHouseSchema(mixedChSchema)
    // a JVM-only UDT in the schema JSON breaks schema parsing in PySpark / Spark Connect clients
    assert(!schema.json.contains("udt"))
    val restored = DataType.fromJson(schema.json).asInstanceOf[StructType]
    assert(restored.fieldNames === schema.fieldNames)
    val restoredField = restored("agg_state")
    assert(restoredField.dataType === NullType)
    assert(restoredField.metadata.getString(ClickHouseUnsupportedType.CLICKHOUSE_TYPE_METADATA_KEY)
      === "AggregateFunction(sum, Int32)")
  }

  test("unsupported columns are still detected after a schema JSON round-trip") {
    // Detection must survive a schema JSON round-trip (PySpark / Spark Connect flattens the UDT to void).
    val schema = fromClickHouseSchema(mixedChSchema)
    val restored = DataType.fromJson(schema.json).asInstanceOf[StructType]

    assert(ClickHouseUnsupportedType.unsupportedColumns(restored) === Seq(
      "agg_state" -> "AggregateFunction(sum, Int32)",
      "avg_state" -> "AggregateFunction(avg, Float64)"
    ))
    val e = intercept[CHClientException](toClickHouseSchema(restored))
    assert(e.getMessage.contains("`agg_state` AggregateFunction(sum, Int32)"))
    assert(e.getMessage.contains("`avg_state` AggregateFunction(avg, Float64)"))
  }

  test("spark2ch - VariantType defaults to JSON") {
    assert(toClickHouseType(VariantType, nullable = false) == "JSON")
  }

  test("spark2ch - VariantType with variant_types emits Variant(...)") {
    assert(
      toClickHouseType(VariantType, nullable = false, Some("String, Int64, JSON")) ==
        "Variant(String, Int64, JSON)"
    )
  }

  test("spark2ch - VariantType with json_hints emits JSON(...)") {
    assert(
      toClickHouseType(VariantType, nullable = false, None, Some("a.b UInt32, SKIP a.c, max_dynamic_paths=16")) ==
        "JSON(a.b UInt32, SKIP a.c, max_dynamic_paths=16)"
    )
  }

  test("spark2ch - variant_types and json_hints are mutually exclusive") {
    val catalystSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("data", VariantType, nullable = false)
    ))
    val ex = intercept[CHClientException] {
      toClickHouseSchema(
        catalystSchema,
        Map(
          "clickhouse.column.data.variant_types" -> "String, Int64",
          "clickhouse.column.data.json_hints" -> "a.b UInt32"
        )
      )
    }
    assert(ex.getMessage.contains("data"))
  }

  test("spark2ch - json_hints flows through table properties") {
    val catalystSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("data", VariantType, nullable = false)
    ))
    assert(Seq(
      ("id", "Int32", ""),
      ("data", "JSON(a.b UInt32, max_dynamic_paths=16)", "")
    ) == toClickHouseSchema(
      catalystSchema,
      Map("clickhouse.column.data.json_hints" -> "a.b UInt32, max_dynamic_paths=16")
    ))
  }
}
