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

import com.clickhouse.ClickHouseHelper
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class ClickHouseHelperSuite extends AnyFunSuite with ClickHouseHelper {

  test("buildNodeSpec") {
    val nodeSpec = buildNodeSpec(
      new CaseInsensitiveStringMap(Map(
        "database" -> "testing",
        "option.database" -> "production",
        "option.ssl" -> "true"
      ).asJava)
    )
    assert(nodeSpec.database === "testing")
    assert(nodeSpec.options.get("ssl") === "true")
  }
}
