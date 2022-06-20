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

package xenon.clickhouse.write.format

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.JsonWriter
import xenon.clickhouse.write.{ClickHouseWriter, WriteJobDescription}

class ClickHouseJsonEachRowWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "JSONEachRow"

  val jsonWriter: JsonWriter = new JsonWriter(revisedDataSchema, writeJob.tz, serializedOutput)

  override def writeRow(record: InternalRow): Unit = jsonWriter.write(record)

  override def serialize(): ByteString = {
    jsonWriter.flush()
    serializedBuffer.toByteString
  }

  override def close(): Unit = {
    jsonWriter.close()
    super.close()
  }
}
