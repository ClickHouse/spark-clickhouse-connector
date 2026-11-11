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

import com.clickhouse.spark.func.{
  ClickHouseXxHash64,
  ClickhouseEquivFunction,
  CompositeFunctionRegistry,
  DynamicFunctionRegistry,
  StaticFunctionRegistry
}
import org.scalatest.funsuite.AnyFunSuite
import com.clickhouse.spark.func._

class FunctionRegistrySuite extends AnyFunSuite {

  val staticFunctionRegistry: StaticFunctionRegistry.type = StaticFunctionRegistry
  val dynamicFunctionRegistry = new DynamicFunctionRegistry
  dynamicFunctionRegistry.register("ck_xx_hash64", ClickHouseXxHash64)
  dynamicFunctionRegistry.register("clickhouse_xxHash64", ClickHouseXxHash64)

  test("check StaticFunctionRegistry mappings") {
    assert(staticFunctionRegistry.sparkToClickHouseFunc.forall { case (k, v) =>
      staticFunctionRegistry.load(k).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(v)
    })
    assert(staticFunctionRegistry.clickHouseToSparkFunc.forall { case (k, v) =>
      staticFunctionRegistry.load(v).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(k)
    })
  }

  test("check DynamicFunctionRegistry mappings") {
    assert(dynamicFunctionRegistry.sparkToClickHouseFunc.forall { case (k, v) =>
      dynamicFunctionRegistry.load(k).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(v)
    })
    assert(dynamicFunctionRegistry.clickHouseToSparkFunc.forall { case (k, v) =>
      dynamicFunctionRegistry.load(v).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(k)
    })
  }

  test("check CompositeFunctionRegistry mappings") {
    val compositeFunctionRegistry =
      new CompositeFunctionRegistry(Array(staticFunctionRegistry, dynamicFunctionRegistry))
    assert(compositeFunctionRegistry.sparkToClickHouseFunc.forall { case (k, v) =>
      compositeFunctionRegistry.load(k).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(v)
    })
    assert(compositeFunctionRegistry.clickHouseToSparkFunc.forall { case (k, v) =>
      compositeFunctionRegistry.load(v).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(k)
    })
  }
}
