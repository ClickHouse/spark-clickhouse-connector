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

package org.apache.spark.sql.clickhouse.single

import com.clickhouse.spark.write.coalesce.{ArrowStreamAssembler, BatchHandle, BytesPayload, ShardKey}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels
import scala.collection.JavaConverters._

class ArrowStreamAssemblerRoundTripSuite extends AnyFunSuite {

  test("header + N record-batch payloads + EOS round-trips through ArrowStreamReader") {
    val allocator = new RootAllocator(Long.MaxValue)
    val field = new Field("n", FieldType.nullable(new ArrowType.Int(32, true)), null)
    val schema = new Schema(List(field).asJava)

    def batchBytes(values: Seq[Int]): Array[Byte] = {
      val root = VectorSchemaRoot.create(schema, allocator)
      val vec = root.getVector("n").asInstanceOf[IntVector]
      vec.allocateNew(values.length)
      values.zipWithIndex.foreach { case (v, i) => vec.setSafe(i, v) }
      root.setRowCount(values.length)
      val baos = new ByteArrayOutputStream()
      val ch = new WriteChannel(Channels.newChannel(baos))
      val rb = new VectorUnloader(root).getRecordBatch
      try MessageSerializer.serialize(ch, rb)
      finally rb.close()
      root.close()
      baos.toByteArray
    }

    val headerBaos = new ByteArrayOutputStream()
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(headerBaos)), schema)
    val header = headerBaos.toByteArray

    val group = Seq(
      BatchHandle(ShardKey("q", "d", "t", None), "1", 2L, new BytesPayload(batchBytes(Seq(1, 2)))),
      BatchHandle(ShardKey("q", "d", "t", None), "2", 3L, new BytesPayload(batchBytes(Seq(3, 4, 5))))
    )
    def readAll(is: java.io.InputStream): Array[Byte] = {
      val baos = new ByteArrayOutputStream(); val tmp = new Array[Byte](4096)
      var n = is.read(tmp); while (n != -1) { baos.write(tmp, 0, n); n = is.read(tmp) }; baos.toByteArray
    }
    val streamBytes = readAll(ArrowStreamAssembler.lazyStream(header, group))

    val reader = new ArrowStreamReader(new ByteArrayInputStream(streamBytes), allocator)
    val readBack = scala.collection.mutable.ArrayBuffer.empty[Int]
    while (reader.loadNextBatch()) {
      val vec = reader.getVectorSchemaRoot.getVector("n").asInstanceOf[IntVector]
      (0 until vec.getValueCount).foreach(i => readBack += vec.get(i))
    }
    reader.close()
    allocator.close()
    assert(readBack.toSeq == Seq(1, 2, 3, 4, 5))
  }
}
