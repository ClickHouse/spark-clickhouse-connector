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

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.StructType

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.time.ZoneId

object JsonFormatUtils {

  private val option = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss"
  )

  // inefficiently
  def row2Json(row: InternalRow, schema: StructType, tz: ZoneId): ByteString = {
    val line = ByteString.newOutput()

    val gen = new JacksonGenerator(
      schema,
      new OutputStreamWriter(line, StandardCharsets.UTF_8),
      new JSONOptions(option, tz.getId)
    )
    gen.write(row)
    gen.writeLineEnding
    gen.flush

    line.toByteString
  }
}
