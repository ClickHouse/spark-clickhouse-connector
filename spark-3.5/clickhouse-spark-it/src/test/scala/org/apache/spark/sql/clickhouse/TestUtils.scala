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

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.ClassTagExtensions

object TestUtils {

  @transient lazy val om: ObjectMapper with ClassTagExtensions = {
    val _om = new ObjectMapper() with ClassTagExtensions
    _om.findAndRegisterModules()
    _om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _om
  }

  def toJson(value: Any): String = om.writeValueAsString(value)
}
