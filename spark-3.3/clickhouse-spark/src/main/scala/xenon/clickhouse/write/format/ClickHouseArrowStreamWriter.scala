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
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.SparkUtils
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.execution.arrow.ArrowWriter
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.io.ObservableOutputStream
import xenon.clickhouse.write.{ClickHouseWriter, WriteJobDescription, WriteTaskMetric}

import java.lang.reflect.Field
import java.util.concurrent.atomic.LongAdder
import java.util.zip.GZIPOutputStream

class ClickHouseArrowStreamWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "ArrowStream"

  val reusedByteArray: ByteString.Output = ByteString.newOutput(16 * 1024 * 1024)

  val allocator: BufferAllocator = SparkUtils.spawnArrowAllocator("writer for ClickHouse")
  val arrowSchema: Schema = SparkUtils.toArrowSchema(revisedDataSchema, writeJob.tz.getId)
  val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
  val arrowWriter: ArrowWriter = ArrowWriter.create(root)
  val countField: Field = arrowWriter.getClass.getDeclaredField("count")
  countField.setAccessible(true)

  override def lastRecordsWritten: Long = countField.getInt(arrowWriter)
  val _lastRawBytesWritten = new LongAdder
  override def lastRawBytesWritten: Long = _lastRawBytesWritten.longValue
  val _totalRawBytesWritten = new LongAdder
  override def totalRawBytesWritten: Long = _totalRawBytesWritten.longValue
  val _lastSerializedBytesWritten = new LongAdder
  override def lastSerializedBytesWritten: Long = _lastSerializedBytesWritten.longValue
  val _totalSerializedBytesWritten = new LongAdder
  override def totalSerializedBytesWritten: Long = _totalSerializedBytesWritten.longValue
  val _serializeTime = new LongAdder
  override def lastSerializeTime: Long = _serializeTime.longValue
  val _totalSerializeTime = new LongAdder
  override def totalSerializeTime: Long = _totalSerializeTime.longValue

  override def currentMetricsValues: Array[CustomTaskMetric] = super.currentMetricsValues ++ Array(
    WriteTaskMetric("flushBufferSize", reusedByteArray.size)
  )

  override def resetBuffer(): Unit = {
    reusedByteArray.reset()
    arrowWriter.reset()
    _lastRawBytesWritten.reset()
    _lastSerializedBytesWritten.reset()
    _serializeTime.reset()
  }

  override def writeRow(record: InternalRow): Unit = arrowWriter.write(record)

  override def serialize(): ByteString = {
    arrowWriter.finish()
    val serializedOutput = new ObservableOutputStream(
      reusedByteArray,
      Some(_lastSerializedBytesWritten),
      Some(_totalSerializedBytesWritten)
    )
    val codecOutput = codec match {
      case None => serializedOutput
      case Some(codec) if codec.toLowerCase == "gzip" =>
        new GZIPOutputStream(serializedOutput, 8192)
      case Some(unsupported) =>
        throw ClickHouseClientException(s"Unsupported compression codec: $unsupported")
    }
    val rawOutput = new ObservableOutputStream(
      codecOutput,
      Some(_lastRawBytesWritten),
      Some(_totalRawBytesWritten),
      Some(_serializeTime),
      Some(_totalSerializeTime)
    )
    val arrowStreamWriter = new ArrowStreamWriter(arrowWriter.root, null, rawOutput)
    arrowStreamWriter.writeBatch()
    arrowStreamWriter.end()
    codecOutput.flush()
    codecOutput.close()
    reusedByteArray.toByteString
  }

  override def close(): Unit = {
    root.close()
    allocator.close()
    super.close()
  }
}
