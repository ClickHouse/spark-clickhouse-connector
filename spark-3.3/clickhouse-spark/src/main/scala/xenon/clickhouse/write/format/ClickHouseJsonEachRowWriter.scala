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
import xenon.clickhouse.io.{ClickHouseLZ4OutputStream, ObservableOutputStream}
import xenon.clickhouse.write.{ClickHouseWriter, WriteJobDescription, WriteTaskMetric}

import java.io.OutputStream
import java.util.concurrent.atomic.LongAdder
import java.util.zip.GZIPOutputStream
import scala.collection.mutable.ArrayBuffer

class ClickHouseJsonEachRowWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "JSONEachRow"

  val reusedByteArray: ByteString.Output = ByteString.newOutput(16 * 1024 * 1024)

  val buf: ArrayBuffer[ByteString] = new ArrayBuffer[ByteString](writeJob.writeOptions.batchSize)
  val jsonWriter = new JsonWriter(revisedDataSchema, writeJob.tz)

  override def lastRecordsWritten: Long = buf.length
  val _lastRawBytesWritten = new LongAdder
  override def lastRawBytesWritten: Long = buf.map(_.size).sum
  val _totalRawBytesWritten = new LongAdder
  override def totalRawBytesWritten: Long = _totalRawBytesWritten.longValue
  val _lastSerializedBytesWritten = new LongAdder
  override def lastSerializedBytesWritten: Long = _lastSerializedBytesWritten.longValue
  val _totalSerializedBytesWritten = new LongAdder
  override def totalSerializedBytesWritten: Long = _totalSerializedBytesWritten.longValue
  val _lastSerializeTime = new LongAdder
  override def lastSerializeTime: Long = _lastSerializeTime.longValue
  val _totalSerializeTime = new LongAdder
  override def totalSerializeTime: Long = _totalSerializeTime.longValue

  override def currentMetricsValues: Array[CustomTaskMetric] = super.currentMetricsValues ++ Array(
    WriteTaskMetric("flushBufferSize", reusedByteArray.size)
  )

  override def resetBuffer(): Unit = {
    reusedByteArray.reset()
    _lastRawBytesWritten.reset()
    _lastSerializedBytesWritten.reset()
    _lastSerializeTime.reset()
    buf.clear()
  }

  override def writeRow(record: InternalRow): Unit = buf += jsonWriter.row2Json(record)

  override def serialize(): ByteString = codec match {
    case None =>
      val (data, serializedTime) = Utils.timeTakenMs(buf.reduce((l, r) => l concat r))
      _lastRawBytesWritten.add(data.size)
      _totalRawBytesWritten.add(data.size)
      _lastSerializedBytesWritten.add(data.size)
      _totalSerializedBytesWritten.add(data.size)
      _lastSerializeTime.add(serializedTime)
      _totalSerializedBytesWritten.add(serializedTime)
      data
    case Some(codec) if codec.toLowerCase == "gzip" =>
      doSerialize(output => new GZIPOutputStream(output, 8192))
    case Some(codec) if codec.toLowerCase == "lz4" =>
      doSerialize(output => new ClickHouseLZ4OutputStream(output, 8192))
    case Some(unsupported) =>
      throw ClickHouseClientException(s"unsupported compression codec: $unsupported")
  }

  private def doSerialize(compress: OutputStream => OutputStream): ByteString = {
    val output = new ObservableOutputStream(
      reusedByteArray,
      Some(_lastRawBytesWritten),
      Some(_totalRawBytesWritten),
      Some(_lastSerializeTime),
      Some(_totalSerializeTime)
    )
    val compressedOutput = compress(output)
    buf.foreach(_.writeTo(compressedOutput))
    compressedOutput.flush()
    compressedOutput.close()
    reusedByteArray.toByteString
  }
}
