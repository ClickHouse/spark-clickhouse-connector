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

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import xenon.clickhouse.ClickHouseHelper
import xenon.clickhouse.func.{
  ClickHouseXxHash64,
  CompositeFunctionRegistry,
  DynamicFunctionRegistry,
  StaticFunctionRegistry
}

import scala.collection.JavaConverters._

class FunctionRegistrySuite extends AnyFunSuite {

  val staticFunctionRegistry: StaticFunctionRegistry.type = StaticFunctionRegistry
  val dynamicFunctionRegistry = new DynamicFunctionRegistry
  dynamicFunctionRegistry.register("ck_xx_hash64", ClickHouseXxHash64)
  dynamicFunctionRegistry.register("clickhouse_xxHash64", ClickHouseXxHash64)

  test("check StaticFunctionRegistry mappings") {
    assert(staticFunctionRegistry.getFuncMappingBySpark === Map(
      "ck_xx_hash64" -> "xxHash64",
      "clickhouse_xxHash64" -> "xxHash64"
    ))
    assert((staticFunctionRegistry.getFuncMappingByCk === Map(
      "xxHash64" -> "clickhouse_xxHash64"
    )) || (staticFunctionRegistry.getFuncMappingByCk === Map(
      "xxHash64" -> "ck_xx_hash64"
    )))
  }

  test("check DynamicFunctionRegistry mappings") {
    assert(dynamicFunctionRegistry.getFuncMappingBySpark === Map(
      "ck_xx_hash64" -> "xxHash64",
      "clickhouse_xxHash64" -> "xxHash64"
    ))
    assert((dynamicFunctionRegistry.getFuncMappingByCk === Map(
      "xxHash64" -> "clickhouse_xxHash64"
    )) || (dynamicFunctionRegistry.getFuncMappingByCk === Map(
      "xxHash64" -> "ck_xx_hash64"
    )))
  }

  test("check CompositeFunctionRegistry mappings") {
    val compositeFunctionRegistry =
      new CompositeFunctionRegistry(Array(staticFunctionRegistry, dynamicFunctionRegistry))
    assert(compositeFunctionRegistry.getFuncMappingBySpark === Map(
      "ck_xx_hash64" -> "xxHash64",
      "clickhouse_xxHash64" -> "xxHash64"
    ))
    assert((compositeFunctionRegistry.getFuncMappingByCk === Map(
      "xxHash64" -> "clickhouse_xxHash64"
    )) || (compositeFunctionRegistry.getFuncMappingByCk === Map(
      "xxHash64" -> "ck_xx_hash64"
    )))
  }
}
