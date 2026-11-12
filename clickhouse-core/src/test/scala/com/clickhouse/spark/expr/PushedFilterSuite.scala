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

package com.clickhouse.spark.expr

import org.scalatest.funsuite.AnyFunSuite

class PushedFilterSuite extends AnyFunSuite {

  test("a filter renders as the SQL it compiled to") {
    assert(PushedFilter.Compare("ts", ">=", "'2024-01-05 00:00:00'").sql === "`ts` >= '2024-01-05 00:00:00'")
    assert(PushedFilter.Compare("`a.b`", "=", "1").sql === "`a.b` = 1")
    assert(PushedFilter.In("m", Seq("1", "2")).sql === "`m` IN (1,2)")
    assert(PushedFilter.Other("(`m` IS NULL)", Set("m")).sql === "(`m` IS NULL)")
  }

  test("a filter names the columns it reads") {
    assert(PushedFilter.Compare("ts", "<", "1").columns === Set("ts"))
    assert(PushedFilter.In("m", Seq("1")).columns === Set("m"))
    assert(PushedFilter.Other("(`a` = `b`)", Set("a", "b")).columns === Set("a", "b"))
  }
}
