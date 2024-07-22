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

package com.clickhouse.spark.spec

import com.clickhouse.spark.ToJson

import java.util
import scala.collection.JavaConverters._

case class DatabaseSpec(
  name: String,
  engine: String,
  data_path: String,
  metadata_path: String,
  uuid: String
) extends ToJson {

  def toMap: Map[String, String] = Map(
    "name" -> name,
    "engine" -> engine,
    "data_path" -> data_path,
    "metadata_path" -> metadata_path,
    "uuid" -> uuid
  )

  def toJavaMap: util.Map[String, String] = toMap.asJava
}
