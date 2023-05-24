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
        val (actualSparkType, actualNullable) = fromClickHouseType(chCols.get(0))
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
    "Decimal256(5)", // overflow
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

  test("spark2ch") {
    val catalystSchema = StructType.fromString(
      """{
        |  "type": "struct",
        |  "fields": [
        |    {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
        |    {"name": "food", "type": "string", "nullable": false, "metadata": {"comment": "food"}},
        |    {"name": "price", "type": "decimal(2,1)", "nullable": false, "metadata": {"comment": "price usd"}},
        |    {"name": "remark", "type": "string", "nullable": true, "metadata": {}}
        |  ]
        |}
        |""".stripMargin
    )
    assert(Seq(
      ("id", "Int32", ""),
      ("food", "String", " COMMENT 'food'"),
      ("price", "Decimal(2, 1)", " COMMENT 'price usd'"),
      ("remark", "Nullable(String)", "")
    ) == toClickHouseSchema(catalystSchema))
  }
}
