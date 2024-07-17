/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under th e License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.write.format

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.JsonWriter
import com.clickhouse.write.{ClickHouseWriter, WriteJobDescription}

class ClickHouseJsonEachRowWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "JSONEachRow"

  val jsonWriter: JsonWriter = new JsonWriter(revisedDataSchema, writeJob.tz, output)

  override def writeRow(record: InternalRow): Unit = jsonWriter.write(record)

  override def doSerialize(): Array[Byte] = {
    jsonWriter.flush()
    output.close()
    serializedBuffer.toByteArray
  }

  override def close(): Unit = {
    IOUtils.closeQuietly(jsonWriter)
    super.close()
  }
}
