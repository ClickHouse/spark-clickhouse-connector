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

import com.clickhouse.spark.SQLHelper
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.aggregate.{Count, Max, Min, Sum}
import org.scalatest.funsuite.AnyFunSuite

class SQLHelperSuite extends AnyFunSuite with SQLHelper {

  test("compileAggregate keeps the plain form when an empty task returns no row") {
    assert(compileAggregate(new Min(FieldReference("id"))) === Some("MIN(`id`)"))
    assert(compileAggregate(new Max(FieldReference("id"))) === Some("MAX(`id`)"))
    assert(compileAggregate(new Sum(FieldReference("id"), false)) === Some("SUM(`id`)"))
    assert(compileAggregate(new Min(FieldReference("tags")), containerColumns = Set("tags")) === Some("MIN(`tags`)"))
  }

  test("compileAggregate makes MIN, MAX and SUM NULL when an empty task still returns a row") {
    // not minOrNull/maxOrNull: those do not match _minmax_count_projection, so MIN of a key
    // column would read the whole column instead of the part metadata
    assert(
      compileAggregate(new Min(FieldReference("id")), nullIfEmpty = true) === Some("if(count() = 0, NULL, MIN(`id`))")
    )
    assert(
      compileAggregate(new Max(FieldReference("id")), nullIfEmpty = true) === Some("if(count() = 0, NULL, MAX(`id`))")
    )
    assert(compileAggregate(new Sum(FieldReference("id"), false), nullIfEmpty = true) === Some("sumOrNull(`id`)"))
    assert(compileAggregate(new Count(FieldReference("id"), false), nullIfEmpty = true) === Some("COUNT(`id`)"))
  }

  test("compileAggregate does not push MIN or MAX of a container column that would need a NULL form") {
    // ClickHouse cannot make a container Nullable before 26.3
    val containers = Set("tags")
    assert(compileAggregate(new Min(FieldReference("tags")), nullIfEmpty = true, containers) === None)
    assert(compileAggregate(new Max(FieldReference("tags")), nullIfEmpty = true, containers) === None)
    assert(
      compileAggregate(new Min(FieldReference("id")), nullIfEmpty = true, containers) ===
        Some("if(count() = 0, NULL, MIN(`id`))")
    )
  }
}
