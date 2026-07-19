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

package com.clickhouse.spark

import org.scalatest.funsuite.AnyFunSuite
import ColumnUtils._

class ColumnUtilsSuite extends AnyFunSuite {

  test("tryParseColumn parses a supported type") {
    val col = tryParseColumn("id", "Nullable(Int32)")
    assert(col.isDefined)
    assert(col.get.getColumnName === "id")
    assert(col.get.isNullable)
  }

  test("tryParseColumn returns None for a type the client can not parse") {
    assert(tryParseColumn("c", "SomeFutureType(42)").isEmpty)
    assert(tryParseColumn("c", "Array(String").isEmpty)
  }

  test("renderColumns renders name and type pairs") {
    assert(renderColumns(Seq("a" -> "Int32", "b" -> "Point")) === "`a` Int32, `b` Point")
    assert(renderColumns(Nil) === "")
  }
}
