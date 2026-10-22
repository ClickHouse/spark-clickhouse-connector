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

import com.clickhouse.client.api.internal.ClickHouseLZ4InputStream
import com.clickhouse.data.ClickHouseColumn

import java.io.InputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.Arrays
import scala.util.control.NonFatal

/**
 * Buffered reader over a RowBinary byte stream, reading values the way client-v2's `BinaryStreamReader` does.
 * Everything read since the last [[mark]] stays buffered, so [[replay]] can hand it to client-v2.
 */
final class RowBinaryInput(in: InputStream, chunkSize: Int = RowBinaryInput.ChunkSize) {
  import RowBinaryInput._

  private var buf = new Array[Byte](chunkSize)
  private var view = littleEndian(buf)
  private var pos = 0
  private var limit = 0
  private var markPos = 0
  private var eof = false
  private var failure: Throwable = _
  // client-v2's LZ4 stream fills a read from as many blocks as it needs, losing the copied bytes if one fails
  private val lz4 = in match {
    case stream: ClickHouseLZ4InputStream => stream
    case _ => null
  }
  private var lz4Left = 0

  /** Array holding the bytes returned by [[take]]; it changes when the buffer grows. */
  def buffer: Array[Byte] = buf

  def mark(): Unit = markPos = pos

  /** Whether the stream threw instead of returning more bytes. */
  def failed: Boolean = failure != null

  /** True if at least one more byte can be read. */
  def hasMore: Boolean = pos < limit || fill(1)

  /** Makes `n` bytes available, or throws [[Unreadable]] if the stream ends or fails first. */
  def require(n: Int): Unit = if (limit - pos < n && !fill(n)) throw Unreadable

  /** Consumes `n` bytes and returns their offset in [[buffer]]. */
  def take(n: Int): Int = {
    require(n)
    val offset = pos
    pos += n
    offset
  }

  def readByte(): Byte = {
    if (pos == limit) require(1)
    val b = buf(pos)
    pos += 1
    b
  }

  def readShortLE(): Short = {
    if (limit - pos < 2) require(2)
    val v = view.getShort(pos)
    pos += 2
    v
  }

  def readIntLE(): Int = {
    if (limit - pos < 4) require(4)
    val v = view.getInt(pos)
    pos += 4
    v
  }

  def readLongLE(): Long = {
    if (limit - pos < 8) require(8)
    val v = view.getLong(pos)
    pos += 8
    v
  }

  /** Same result as client-v2's `readVarInt`: up to 10 bytes accumulated into an Int. */
  def readVarInt(): Int = {
    if (pos < limit && buf(pos) >= 0) {
      val b = buf(pos)
      pos += 1
      return b
    }
    var value = 0
    var i = 0
    while (i < 10) {
      val b = readByte()
      value |= (b & 0x7f) << (7 * i)
      if ((b & 0x80) == 0) return value
      i += 1
    }
    value
  }

  /** Same result as client-v2's `readString`; throws [[Unreadable]] for a negative length. */
  def readString(): String = {
    val len = readVarInt()
    if (len < 0) throw Unreadable
    if (len == 0) ""
    else {
      val offset = take(len)
      new String(buf, offset, len, StandardCharsets.UTF_8)
    }
  }

  /** Whether the `length` bytes at `offset` in [[buffer]] are all ASCII. */
  def isAscii(offset: Int, length: Int): Boolean = {
    val end = offset + length
    var i = offset
    while (i + 8 <= end) {
      if ((view.getLong(i) & 0x8080808080808080L) != 0) return false
      i += 8
    }
    while (i < end) {
      if (buf(i) < 0) return false
      i += 1
    }
    true
  }

  /** Reads a RowBinaryWithNamesAndTypes header, or None unless it holds `columns` columns that parse. */
  def readHeader(columns: Int): Option[Array[ClickHouseColumn]] =
    try
      if (readVarInt() != columns) None
      else {
        val names = Array.fill(columns)(readString())
        val types = Array.fill(columns)(readString())
        Some(Array.tabulate(columns)(i => ClickHouseColumn.of(names(i), types(i))))
      }
    catch {
      case NonFatal(_) => None
    }

  /** Copies the bytes read since the mark. */
  def markedBytes(): Array[Byte] = Arrays.copyOfRange(buf, markPos, pos)

  /**
   * Stream of `prefix`, the bytes buffered since the mark, then the rest of the input, ending or failing where this
   * reader's input did. This reader must not be used afterwards.
   */
  def replay(prefix: Array[Byte]): InputStream = {
    val kept = limit - markPos
    val head = Arrays.copyOf(prefix, prefix.length + kept)
    System.arraycopy(buf, markPos, head, prefix.length, kept)
    buf = null
    view = null
    new ReplayInputStream(head, in, eof, failure)
  }

  // One method on purpose: split up, the JIT inlined it into every read and decoding got ~20% slower
  private def fill(n: Int): Boolean = {
    while (limit - pos < n) {
      if (eof || failure != null) return false
      if (limit == buf.length) {
        // move the bytes from the mark to the front; grow the buffer when they fill it, shrink it after a large row
        val needed = (pos - markPos).toLong + n
        if (needed > MaxBufferSize) return false
        val kept = limit - markPos
        val target =
          if (needed > buf.length) new Array[Byte](math.min(buf.length * 2L, MaxBufferSize).toInt)
          else if (needed <= chunkSize && buf.length > chunkSize) new Array[Byte](chunkSize)
          else buf
        System.arraycopy(buf, markPos, target, 0, kept)
        if (target ne buf) {
          buf = target
          view = littleEndian(buf)
        }
        pos -= markPos
        limit = kept
        markPos = 0
      }
      // from the LZ4 stream no more than its current block, so it fails where client-v2's reads would; one byte makes
      // it decompress the next block, whose size its header then holds
      val len =
        if (lz4 == null) buf.length - limit else if (lz4Left > 0) math.min(buf.length - limit, lz4Left) else 1
      val read =
        try in.read(buf, limit, len)
        catch {
          case NonFatal(e) =>
            failure = e
            return false
        }
      if (read > 0 && lz4 != null) {
        lz4Left = (if (lz4Left > 0) lz4Left else littleEndian(lz4.getHeaderBuffer).getInt(21)) - read
      }
      if (read < 0) eof = true else limit += read
    }
    true
  }
}

object RowBinaryInput {
  val ChunkSize: Int = 64 * 1024

  private val MaxBufferSize = Int.MaxValue - 16

  /** The rest of the current row can't be read here: the stream ended or failed, or a value needs client-v2. */
  object Unreadable extends RuntimeException(null, null, false, false)

  private def littleEndian(buf: Array[Byte]): ByteBuffer = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)

  final private class ReplayInputStream(
    private var head: Array[Byte],
    tail: InputStream,
    ended: Boolean,
    failure: Throwable
  ) extends InputStream {
    private var pos = 0

    // drops the replayed bytes once read
    private def consumed(n: Int): Unit = {
      pos += n
      if (pos == head.length) {
        head = Array.emptyByteArray
        pos = 0
      }
    }

    override def read(): Int =
      if (pos < head.length) {
        val b = head(pos) & 0xff
        consumed(1)
        b
      } else if (failure != null) throw failure
      else if (ended) -1
      else tail.read()

    override def read(b: Array[Byte], off: Int, len: Int): Int =
      if (len == 0) 0
      else if (pos < head.length) {
        val n = math.min(len, head.length - pos)
        System.arraycopy(head, pos, b, off, n)
        consumed(n)
        n
      } else if (failure != null) throw failure
      else if (ended) -1
      else tail.read(b, off, len)

    override def available(): Int = head.length - pos

    override def close(): Unit = tail.close()
  }
}
