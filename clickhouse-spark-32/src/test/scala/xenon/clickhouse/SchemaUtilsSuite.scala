/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse

import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.SchemaUtils._

class SchemaUtilsSuite extends AnyFunSuite {
  test("regex ArrayType") {
    "Array(String)" match {
      case arrayTypePattern(nestType) => assert("String" == nestType)
      case _ => fail()
    }

    "Array(Nullable(String))" match {
      case arrayTypePattern(nestType) => assert("Nullable(String)" == nestType)
      case _ => fail()
    }

    "Array(Array(String))" match {
      case arrayTypePattern(nestType) => assert("Array(String)" == nestType)
      case _ => fail()
    }

    "array(String)" match {
      case arrayTypePattern(_) => fail()
      case _ =>
    }

    "Array(String" match {
      case arrayTypePattern(_) => fail()
      case _ =>
    }
  }

  test("regex DateType") {
    "Date" match {
      case dateTypePattern() =>
      case _ => fail()
    }

    "DT" match {
      case dateTypePattern(_) => fail()
      case _ =>
    }
  }

  test("regex DateTimeType") {
    "DateTime" match {
      case dateTimeTypePattern(_, _, _) =>
      case _ => fail()
    }

    "DateTime(Asia/Shanghai)" match {
      case dateTimeTypePattern(_, _, tz) => assert("Asia/Shanghai" == tz)
      case _ => fail()
    }

    "DateTime64" match {
      case dateTimeTypePattern(_64, _, _) => assert("64" == _64)
      case _ => fail()
    }

    "DateTime64(Europe/Moscow)" match {
      case dateTimeTypePattern(_64, _, tz) =>
        assert("64" == _64)
        assert("Europe/Moscow" == tz)
      case _ => fail()
    }

    "DT" match {
      case dateTimeTypePattern(_) => fail()
      case _ =>
    }
  }

  test("DecimalType") {
    "Decimal(1,2)" match {
      case decimalTypePattern(p, s) =>
        assert("1" == p)
        assert("2" == s)
      case _ => fail()
    }

    "Decimal" match {
      case decimalTypePattern(_, _) => fail()
      case _ =>
    }

    "Decimal(String" match {
      case decimalTypePattern(_, _) => fail()
      case _ =>
    }
  }

  test("regex DecimalType - 2") {
    "Decimal32(5)" match {
      case decimalTypePattern2(a, s) => assert(("32", "5") == (a, s))
      case _ => fail()
    }

    "Decimal64(5)" match {
      case decimalTypePattern2(a, s) => assert(("64", "5") == (a, s))
      case _ => fail()
    }

    "Decimal128(5)" match {
      case decimalTypePattern2(a, s) => assert(("128", "5") == (a, s))
      case _ => fail()
    }

    "Decimal256(5)" match {
      case decimalTypePattern2(a, s) => assert(("256", "5") == (a, s))
      case _ => fail()
    }

    "Decimal32(5" match {
      case decimalTypePattern2(a, s) => fail()
      case _ =>
    }
  }

  test("regex FixedStringType") {
    "FixedString(5)" match {
      case fixedStringTypePattern(l) => assert("5" == l)
      case _ => fail()
    }

    "fixedString(5)" match {
      case fixedStringTypePattern(_) => fail()
      case _ =>
    }

    "String" match {
      case decimalTypePattern2(a, s) => fail()
      case _ =>
    }
  }

  test("testNullableTypeRegex") {
    assert(("String", true) == unwrapNullable("Nullable(String)"))
    assert(("nullable(String)", false) == unwrapNullable("nullable(String)"))
    assert(("String", false) == unwrapNullable("String"))
  }
}
