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

package xenon.clickhouse.func

import org.apache.spark.sql.connector.catalog.functions.UnboundFunction

import scala.collection.mutable

trait FunctionRegistry extends Serializable {

  def list: Array[String]

  def load(name: String): Option[UnboundFunction]

  def sparkToClickHouseFunc: Map[String, String]

  def clickHouseToSparkFunc: Map[String, String]
}

trait ClickhouseEquivFunction {
  val ckFuncNames: Array[String]
}

class CompositeFunctionRegistry(registries: Array[FunctionRegistry]) extends FunctionRegistry {

  override def list: Array[String] = registries.flatMap(_.list)

  override def load(name: String): Option[UnboundFunction] = registries.flatMap(_.load(name)).headOption

  override def sparkToClickHouseFunc: Map[String, String] = registries.flatMap(_.sparkToClickHouseFunc).toMap

  override def clickHouseToSparkFunc: Map[String, String] = registries.flatMap(_.clickHouseToSparkFunc).toMap
}

object StaticFunctionRegistry extends FunctionRegistry {

  private val functions = Map[String, UnboundFunction](
    "ck_xx_hash64" -> ClickHouseXxHash64, // for compatible
    "clickhouse_xxHash64" -> ClickHouseXxHash64,
    "clickhouse_murmurHash2_32" -> MurmurHash2_32,
    "clickhouse_murmurHash2_64" -> MurmurHash2_64,
    "clickhouse_murmurHash3_32" -> MurmurHash3_32,
    "clickhouse_murmurHash3_64" -> MurmurHash3_64,
    "clickhouse_cityHash64" -> CityHash64,
    "clickhouse_years" -> Years,
    "clickhouse_months" -> Months,
    "clickhouse_days" -> Days,
    "clickhouse_hours" -> Hours,
    "sharding_mod" -> Mod
  )

  override def list: Array[String] = functions.keys.toArray

  override def load(name: String): Option[UnboundFunction] = functions.get(name)

  override val sparkToClickHouseFunc: Map[String, String] =
    functions.filter(_._2.isInstanceOf[ClickhouseEquivFunction]).flatMap { case (k, v) =>
      v.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.map((k, _))
    }

  override val clickHouseToSparkFunc: Map[String, String] =
    functions.filter(_._2.isInstanceOf[ClickhouseEquivFunction]).flatMap { case (k, v) =>
      v.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.map((_, k))
    }
}

class DynamicFunctionRegistry extends FunctionRegistry {

  private val functions = mutable.Map[String, UnboundFunction]()

  def register(name: String, function: UnboundFunction): DynamicFunctionRegistry = {
    functions += (name -> function)
    this
  }

  override def list: Array[String] = functions.keys.toArray

  override def load(name: String): Option[UnboundFunction] = functions.get(name)

  override def sparkToClickHouseFunc: Map[String, String] =
    functions.filter(_._2.isInstanceOf[ClickhouseEquivFunction]).toMap.flatMap { case (k, v) =>
      v.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.map((k, _))
    }

  override def clickHouseToSparkFunc: Map[String, String] =
    functions.filter(_._2.isInstanceOf[ClickhouseEquivFunction]).toMap.flatMap { case (k, v) =>
      v.asInstanceOf[ClickhouseEquivFunction].ckFuncNames.map((_, k))
    }
}
