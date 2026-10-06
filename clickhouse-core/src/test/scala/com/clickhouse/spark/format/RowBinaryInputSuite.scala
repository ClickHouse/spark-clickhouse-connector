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

package com.clickhouse.spark.format

import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException, InputStream}
import java.nio.{ByteBuffer, ByteOrder}

class RowBinaryInputSuite extends AnyFunSuite {

  // Hands out at most one byte per read, so every multi-byte value straddles a refill
  private class TrickleInputStream(bytes: Array[Byte]) extends InputStream {
    private val in = new ByteArrayInputStream(bytes)
    override def read(): Int = in.read()
    override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, math.min(len, 1))
  }

  private def littleEndian(size: Int)(put: ByteBuffer => Unit): Array[Byte] = {
    val bb = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)
    put(bb)
    bb.array()
  }

  private val values: Array[Byte] = littleEndian(27) { bb =>
    bb.put(0x81.toByte).putShort(-2).putInt(-3).putLong(Long.MinValue).putFloat(1.5f).putDouble(-2.25)
  }

  private def assertValues(in: RowBinaryInput): Unit = {
    assert(in.readByte() === 0x81.toByte)
    assert(in.readShort() === -2)
    assert(in.readInt() === -3)
    assert(in.readLong() === Long.MinValue)
    assert(in.readFloat() === 1.5f)
    assert(in.readDouble() === -2.25)
  }

  test("reads little-endian values, also when they straddle refills of a small buffer") {
    for (bufferSize <- Seq(1, 2, 3, 5, 8, 64 * 1024)) {
      val in = new RowBinaryInput(new TrickleInputStream(values), bufferSize)
      in.release()
      assertValues(in)
      assert(!in.hasMore)
    }
  }

  test("reads varints and byte runs longer than the buffer") {
    val out = new ByteArrayOutputStream()
    out.write(Array[Byte](0x00, 0x7f, 0x80.toByte, 0x01, 0xff.toByte, 0xff.toByte, 0xff.toByte, 0xff.toByte, 0x07))
    val payload = Array.tabulate[Byte](1000)(_.toByte)
    out.write(payload)
    val in = new RowBinaryInput(new TrickleInputStream(out.toByteArray), 4)
    in.release()
    assert(in.readVarInt() === 0)
    assert(in.readVarInt() === 127)
    assert(in.readVarInt() === 128)
    assert(in.readVarInt() === Int.MaxValue)
    assert(in.readBytes(1000).sameElements(payload))
    assert(!in.hasMore)
  }

  test("replays the whole stream until released") {
    val in = new RowBinaryInput(new TrickleInputStream(values), 2)
    assert(in.readByte() === 0x81.toByte)
    assert(in.readShort() === -2)
    assertValues(new RowBinaryInput(in.replay()))
  }

  test("replay is refused once consumed bytes were released") {
    val in = new RowBinaryInput(new ByteArrayInputStream(values))
    in.release()
    assertThrows[IllegalArgumentException](in.replay())
  }

  test("fails on a truncated value and reports a clean end of stream") {
    val in = new RowBinaryInput(new ByteArrayInputStream(values.take(5)))
    in.release()
    assert(in.hasMore)
    assert(in.readByte() === 0x81.toByte)
    assert(in.readShort() === -2)
    assertThrows[EOFException](in.readInt())
    assert(!new RowBinaryInput(new ByteArrayInputStream(Array.emptyByteArray)).hasMore)
  }
}
