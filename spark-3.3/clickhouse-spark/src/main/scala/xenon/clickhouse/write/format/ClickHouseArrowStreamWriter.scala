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
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.write.{ClickHouseWriter, WriteJobDescription}

import java.lang.reflect.Field
import java.util.zip.GZIPOutputStream

class ClickHouseArrowStreamWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "ArrowStream"

  val reusedByteArray: ByteString.Output = ByteString.newOutput(16 * 1024 * 1024)

  val arrowWriter: ArrowWriter = ArrowWriter.create(revisedDataSchema, writeJob.tz.getId)
  val countField: Field = arrowWriter.getClass.getDeclaredField("count")
  countField.setAccessible(true)

  override def bufferedRows: Int = countField.getInt(arrowWriter)
  override def bufferedBytes: Long = reusedByteArray.size
  override def resetBuffer(): Unit = {
    reusedByteArray.reset()
    arrowWriter.reset()
  }

  override def writeRow(record: InternalRow): Unit = arrowWriter.write(record)

  override def serialize(): ByteString = {
    arrowWriter.finish()
    val out = codec match {
      case None => reusedByteArray
      case Some(codec) if codec.toLowerCase == "gzip" =>
        new GZIPOutputStream(reusedByteArray, 8192)
      case Some(unsupported) =>
        throw ClickHouseClientException(s"Unsupported compression codec: $unsupported")
    }
    val arrowStreamWriter = new ArrowStreamWriter(arrowWriter.root, null, out)
    arrowStreamWriter.writeBatch()
    arrowStreamWriter.end()
    out.flush()
    reusedByteArray.toByteString
  }
}
