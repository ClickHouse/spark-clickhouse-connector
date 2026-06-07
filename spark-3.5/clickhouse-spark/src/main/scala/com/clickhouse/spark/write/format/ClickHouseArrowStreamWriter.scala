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

package com.clickhouse.spark.write.format

import com.clickhouse.spark.write.{ClickHouseWriter, WriteJobDescription}
import com.clickhouse.spark.write.coalesce.{BatchPayload, BytesPayload}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.SparkUtils
import org.apache.spark.sql.execution.arrow.ArrowWriter

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

class ClickHouseArrowStreamWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "ArrowStream"

  val allocator: BufferAllocator = SparkUtils.spawnArrowAllocator("writer for ClickHouse")
  val arrowSchema: Schema = SparkUtils.toArrowSchema(revisedDataSchema, writeJob.tz.getId)
  val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
  val arrowWriter: ArrowWriter = ArrowWriter.create(root)

  override def writeRow(record: InternalRow): Unit = arrowWriter.write(record)

  override def doSerialize(): Array[Byte] = {
    arrowWriter.finish()
    val arrowStreamWriter = new ArrowStreamWriter(root, null, output)
    arrowStreamWriter.writeBatch()
    arrowStreamWriter.end()
    output.flush()
    output.close()
    serializedBuffer.toByteArray
  }

  // IPC schema message for arrowSchema, serialized once and reused as the coalesced stream header.
  override lazy val schemaHeaderBytes: Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val ch = new WriteChannel(Channels.newChannel(baos))
    MessageSerializer.serialize(ch, arrowSchema)
    baos.toByteArray
  }

  // Reused across batches. ByteArrayOutputStream.reset() keeps the backing array, so after the
  // first batch there is no re-grow/doubling churn. Lazy so the direct (non-coalesce) path, which
  // never calls serializeRecordBatch, doesn't allocate it.
  private lazy val recordBatchBuffer = new ByteArrayOutputStream(4 * 1024 * 1024)
  private lazy val recordBatchUnloader = new VectorUnloader(root)

  // Serialize ONLY the current batch's record-batch message (uncompressed). Mirrors what
  // ArrowStreamWriter.writeBatch() emits, minus the schema/EOS framing (added by the assembler).
  override def serializeRecordBatch(): BatchPayload = {
    arrowWriter.finish()
    recordBatchBuffer.reset()
    val ch = new WriteChannel(Channels.newChannel(recordBatchBuffer))
    val recordBatch = recordBatchUnloader.getRecordBatch
    try MessageSerializer.serialize(ch, recordBatch)
    finally recordBatch.close()
    // Copy required: the payload is held by the coordinator and read on the sender thread after
    // this buffer is reset/refilled for the next batch (cross-thread ownership).
    new BytesPayload(recordBatchBuffer.toByteArray)
  }

  override def coalesceSupported: Boolean = true

  override def reset(): Unit = {
    super.reset()
    arrowWriter.reset()
  }

  override def close(): Unit = {
    root.close()
    allocator.close()
    super.close()
  }
}
