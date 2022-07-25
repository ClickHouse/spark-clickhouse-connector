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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.StructType
import xenon.clickhouse.io.ForwardingWriter

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.ZoneId

class JsonWriter(schema: StructType, tz: ZoneId) {

  val option: Map[String, String] = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss"
  )

  val forwardingWriter = new ForwardingWriter

  val gen = new JacksonGenerator(
    schema,
    forwardingWriter,
    new JSONOptions(option, tz.getId)
  )

  def row2Json(row: InternalRow): Array[Byte] = {
    val line = new ByteArrayOutputStream()
    forwardingWriter.updateDelegate(new OutputStreamWriter(line, StandardCharsets.UTF_8))
    gen.write(row)
    gen.writeLineEnding
    gen.flush

    line.toByteArray
  }
}
