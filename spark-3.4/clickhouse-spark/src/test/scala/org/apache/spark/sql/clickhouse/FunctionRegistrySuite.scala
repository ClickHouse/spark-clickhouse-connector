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
  ClickhouseEquivFunction,
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
    assert(staticFunctionRegistry.getFuncMappingBySpark.forall { case (k, v) =>
      staticFunctionRegistry.load(k).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(v)
    })
    assert(staticFunctionRegistry.getFuncMappingByCk.forall { case (k, v) =>
      staticFunctionRegistry.load(v).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(k)
    })
  }

  test("check DynamicFunctionRegistry mappings") {
    assert(dynamicFunctionRegistry.getFuncMappingBySpark.forall { case (k, v) =>
      dynamicFunctionRegistry.load(k).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(v)
    })
    assert(dynamicFunctionRegistry.getFuncMappingByCk.forall { case (k, v) =>
      dynamicFunctionRegistry.load(v).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(k)
    })
  }

  test("check CompositeFunctionRegistry mappings") {
    val compositeFunctionRegistry =
      new CompositeFunctionRegistry(Array(staticFunctionRegistry, dynamicFunctionRegistry))
    assert(compositeFunctionRegistry.getFuncMappingBySpark.forall { case (k, v) =>
      compositeFunctionRegistry.load(k).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(v)
    })
    assert(compositeFunctionRegistry.getFuncMappingByCk.forall { case (k, v) =>
      compositeFunctionRegistry.load(v).get.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.contains(k)
    })
  }
}
