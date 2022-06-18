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
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.write.{ClickHouseWriter, WriteJobDescription}

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import scala.collection.mutable.ArrayBuffer

class ClickHouseJsonEachRowWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "JSONEachRow"

  val buf: ArrayBuffer[ByteString] = new ArrayBuffer[ByteString](writeJob.writeOptions.batchSize)
  val jsonWriter = new JsonWriter(writeJob.dataSetSchema, writeJob.tz)

  override def bufferedRows: Int = buf.length
  override def bufferedBytes: Long = buf.map(_.size).sum
  override def resetBuffer(): Unit = buf.clear()

  override def writeRow(record: InternalRow): Unit = buf += jsonWriter.row2Json(record)

  override def serialize(): ByteString = codec match {
    case None => buf.reduce((l, r) => l concat r)
    case Some(codec) if codec.toLowerCase == "gzip" =>
      val out = new ByteArrayOutputStream(4096)
      val gzipOut = new GZIPOutputStream(out, 8192)
      buf.foreach(_.writeTo(gzipOut))
      gzipOut.flush()
      gzipOut.close()
      ByteString.copyFrom(out.toByteArray)
    case Some(unsupported) =>
      throw ClickHouseClientException(s"unsupported compression codec: $unsupported")
  }
}
