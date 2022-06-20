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

import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.ZoneId

class JsonWriter(schema: StructType, tz: ZoneId, output: OutputStream) {
  private val option: Map[String, String] = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
    "timestampNTZFormat" -> "yyyy-MM-dd HH:mm:ss"
  )
  private val utf8Writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)
  private val jsonWriter = new JacksonGenerator(schema, utf8Writer, new JSONOptions(option, tz.getId))

  def write(row: InternalRow): Unit = {
    jsonWriter.write(row)
    jsonWriter.writeLineEnding
  }

  def flush(): Unit = jsonWriter.flush()

  def close(): Unit = jsonWriter.close()
}
