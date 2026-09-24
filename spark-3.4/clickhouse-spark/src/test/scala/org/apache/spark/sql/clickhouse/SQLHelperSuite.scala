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
import org.apache.spark.sql.connector.expressions.aggregate.{Max, Min}
import org.scalatest.funsuite.AnyFunSuite

class SQLHelperSuite extends AnyFunSuite with SQLHelper {

  test("compileAggregate pushes MIN and MAX as -OrNull") {
    assert(compileAggregate(new Min(FieldReference("id"))) === Some("minOrNull(`id`)"))
    assert(compileAggregate(new Max(FieldReference("id"))) === Some("maxOrNull(`id`)"))
  }

  test("compileAggregate keeps MIN and MAX plain for a container column") {
    // `-OrNull` returns Nullable(T), and ClickHouse cannot nest Array, Map or Tuple in Nullable
    // before 26.3: pushing it there fails and the whole column is read into Spark instead
    val containers = Set("tags")
    assert(compileAggregate(new Min(FieldReference("tags")), containers) === Some("min(`tags`)"))
    assert(compileAggregate(new Max(FieldReference("tags")), containers) === Some("max(`tags`)"))
    // a scalar column named in the same aggregation is unaffected
    assert(compileAggregate(new Min(FieldReference("id")), containers) === Some("minOrNull(`id`)"))
  }
}
