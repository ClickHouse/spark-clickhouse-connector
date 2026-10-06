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

import java.io.{ByteArrayInputStream, EOFException, InputStream, SequenceInputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays

/**
 * Buffered little-endian reader for a ClickHouse RowBinary stream.
 *
 * Until [[release]] is called every byte stays buffered, so [[replay]] can hand the whole stream to
 * another reader.
 */
class RowBinaryInput(in: InputStream, initialBufferSize: Int = 64 * 1024) {
  private var buf: Array[Byte] = new Array[Byte](initialBufferSize)
  private var bb: ByteBuffer = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
  private var pos: Int = 0
  private var limit: Int = 0
  private var retaining: Boolean = true

  /** Stops keeping consumed bytes; [[replay]] is no longer possible. */
  def release(): Unit = retaining = false

  /** The stream from its first byte, including the bytes already read. */
  def replay(): InputStream = {
    require(retaining, "consumed bytes were already released")
    new SequenceInputStream(new ByteArrayInputStream(buf, 0, limit), in)
  }

  /** Whether the stream has another byte, i.e. has not ended. */
  def hasMore: Boolean = pos < limit || fill(1)

  def readByte(): Byte = {
    ensure(1)
    val v = buf(pos)
    pos += 1
    v
  }

  def readShort(): Short = {
    ensure(2)
    val v = bb.getShort(pos)
    pos += 2
    v
  }

  def readInt(): Int = {
    ensure(4)
    val v = bb.getInt(pos)
    pos += 4
    v
  }

  def readLong(): Long = {
    ensure(8)
    val v = bb.getLong(pos)
    pos += 8
    v
  }

  def readFloat(): Float = {
    ensure(4)
    val v = bb.getFloat(pos)
    pos += 4
    v
  }

  def readDouble(): Double = {
    ensure(8)
    val v = bb.getDouble(pos)
    pos += 8
    v
  }

  /** Unsigned LEB128, as ClickHouse writes lengths. */
  def readVarInt(): Int = {
    var value = 0
    var shift = 0
    var b = 0
    do {
      b = readByte()
      value |= (b & 0x7f) << shift
      shift += 7
    } while ((b & 0x80) != 0 && shift < 70)
    value
  }

  def readBytes(len: Int): Array[Byte] = {
    ensure(len)
    val v = Arrays.copyOfRange(buf, pos, pos + len)
    pos += len
    v
  }

  private def ensure(n: Int): Unit =
    if (limit - pos < n && !fill(n)) throw new EOFException("Unexpected end of RowBinary stream")

  // Buffers at least n unread bytes, or returns false if the stream ends first.
  private def fill(n: Int): Boolean = {
    if (retaining) {
      if (pos + n > buf.length) grow(pos + n)
    } else {
      if (pos > 0) {
        System.arraycopy(buf, pos, buf, 0, limit - pos)
        limit -= pos
        pos = 0
      }
      if (n > buf.length) grow(n)
    }
    while (limit - pos < n) {
      val read = in.read(buf, limit, buf.length - limit)
      if (read < 0) return false
      limit += read
    }
    true
  }

  private def grow(minCapacity: Int): Unit = {
    buf = Arrays.copyOf(buf, math.max(minCapacity, buf.length * 2))
    bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
  }
}
