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

package com.clickhouse.spark

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.ClassTagExtensions

trait ToJson {

  def toJson: String = JsonProtocol.toJson(this)

  override def toString: String = toJson
}

trait FromJson[T] {
  def fromJson(json: String): T
}

trait JsonProtocol[T] extends FromJson[T] with ToJson

object JsonProtocol {

  @transient lazy val om: ObjectMapper with ClassTagExtensions = {
    val _om = new ObjectMapper() with ClassTagExtensions
    _om.findAndRegisterModules()
    _om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _om
  }

  def toJson(value: Any): String = om.writeValueAsString(value)

  def toPrettyJson(value: Any): String = om.writer(SerializationFeature.INDENT_OUTPUT).writeValueAsString(value)
}
