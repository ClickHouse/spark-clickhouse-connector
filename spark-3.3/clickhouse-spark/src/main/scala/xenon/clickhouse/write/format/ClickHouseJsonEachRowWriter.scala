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
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import xenon.clickhouse.Utils
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.io.ObservableOutputStream
import xenon.clickhouse.write.{ClickHouseWriter, WriteJobDescription, WriteTaskMetric}

import java.util.concurrent.atomic.LongAdder
import java.util.zip.GZIPOutputStream
import scala.collection.mutable.ArrayBuffer

class ClickHouseJsonEachRowWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "JSONEachRow"

  val reusedByteArray: ByteString.Output = ByteString.newOutput(16 * 1024 * 1024)

  val buf: ArrayBuffer[ByteString] = new ArrayBuffer[ByteString](writeJob.writeOptions.batchSize)
  val jsonWriter = new JsonWriter(revisedDataSchema, writeJob.tz)

  val rawBytesWrittenCounter = new LongAdder
  val totalRawBytesWrittenCounter = new LongAdder
  val serializedBytesWrittenCounter = new LongAdder
  val totalSerializedBytesWrittenCounter = new LongAdder
  val serializeTimeCounter = new LongAdder
  val totalSerializeTimeCounter = new LongAdder

  override def totalRawBytesWritten: Long = totalRawBytesWrittenCounter.longValue
  override def totalSerializedBytesWritten: Long = totalSerializedBytesWrittenCounter.longValue
  override def totalSerializeTime: Long = totalSerializeTimeCounter.longValue

  override def currentMetricsValues: Array[CustomTaskMetric] = super.currentMetricsValues ++ Array(
    WriteTaskMetric("flushBufferSize", reusedByteArray.size)
  )

  override def bufferedRows: Int = buf.length
  override def bufferedBytes: Long = buf.map(_.size).sum
  override def resetBuffer(): Unit = {
    reusedByteArray.reset()
    buf.clear()
  }

  override def writeRow(record: InternalRow): Unit = buf += jsonWriter.row2Json(record)

  override def serialize(): ByteString = codec match {
    case None =>
      val (data, serializedTime) = Utils.timeTakenMs(buf.reduce((l, r) => l concat r))
      rawBytesWrittenCounter.add(data.size)
      totalRawBytesWrittenCounter.add(data.size)
      serializedBytesWrittenCounter.add(data.size)
      totalSerializedBytesWrittenCounter.add(data.size)
      serializeTimeCounter.add(serializedTime)
      totalSerializedBytesWrittenCounter.add(serializedTime)
      data
    case Some(codec) if codec.toLowerCase == "gzip" =>
      val output = new ObservableOutputStream(
        reusedByteArray,
        Some(rawBytesWrittenCounter),
        Some(totalRawBytesWrittenCounter),
        Some(serializeTimeCounter),
        Some(totalSerializeTimeCounter)
      )
      val gzipOut = new GZIPOutputStream(output, 8192)
      buf.foreach(_.writeTo(gzipOut))
      gzipOut.flush()
      gzipOut.close()
      reusedByteArray.toByteString
    case Some(unsupported) =>
      throw ClickHouseClientException(s"unsupported compression codec: $unsupported")
  }
}
