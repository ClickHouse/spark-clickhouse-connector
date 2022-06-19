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

package xenon.clickhouse.io

import net.jpountz.lz4.{LZ4Compressor, LZ4Factory}
import xenon.clickhouse.ClickHouseCityHash
import xenon.clickhouse.io.ClickHouseLz4OutputStream._

import java.io.OutputStream

object ClickHouseLz4OutputStream {
  val COMPRESSION_HEADER_LENGTH = 9
  val CHECKSUM_LENGTH = 16
  val lz4Compressor: LZ4Compressor = LZ4Factory.fastestInstance.fastCompressor
}

class ClickHouseLz4OutputStream(output: OutputStream, bufferLength: Int) extends OutputStream {

  private val uncompressedBuffer = new Array[Byte](bufferLength)
  private var pos = 0

  override def write(byt: Int): Unit = {
    uncompressedBuffer(pos) = (byt & 0xff).toByte
    pos += 1
    flush(false)
  }

  override def write(bytes: Array[Byte]): Unit =
    write(bytes, 0, bytes.length)

  override def write(bytes: Array[Byte], _offset: Int, _length: Int): Unit = {
    var (offset, length) = (_offset, _length)
    while (remaining < length) {
      val num = remaining
      System.arraycopy(bytes, offset, uncompressedBuffer, pos, remaining)
      pos += num
      flush(false)
      offset += num
      length -= num
    }
    System.arraycopy(bytes, offset, uncompressedBuffer, pos, length)
    pos += length
    flush(false)
  }

  override def flush(): Unit = flush(true)

  override def close(): Unit = {
    flush()
    output.close()
  }

  private def flush(force: Boolean): Unit =
    if (pos > 0 && (force || !hasRemaining)) {
      val maxLen = lz4Compressor.maxCompressedLength(pos)
      val compressedBuffer = new Array[Byte](maxLen + COMPRESSION_HEADER_LENGTH + CHECKSUM_LENGTH)
      val res = lz4Compressor.compress(uncompressedBuffer, 0, pos, compressedBuffer, 9 + 16)
      compressedBuffer(CHECKSUM_LENGTH) = (0x82 & 0xff).toByte
      val compressedSize = res + COMPRESSION_HEADER_LENGTH
      System.arraycopy(littleEndian(compressedSize), 0, compressedBuffer, CHECKSUM_LENGTH + 1, 4)
      System.arraycopy(littleEndian(pos), 0, compressedBuffer, 21, 4)
      val checksum = ClickHouseCityHash.cityHash128(compressedBuffer, 16, compressedSize)
      System.arraycopy(littleEndian(checksum(0)), 0, compressedBuffer, 0, 8)
      System.arraycopy(littleEndian(checksum(1)), 0, compressedBuffer, 8, 8)
      output.write(compressedBuffer, 0, compressedSize + CHECKSUM_LENGTH)
      pos = 0
    }

  private def hasRemaining = pos < bufferLength

  private def remaining: Int = bufferLength - pos

  private def littleEndian(x: Int): Array[Byte] = {
    val data = new Array[Byte](4)
    data(0) = ((x >> 0).toByte & 0xff).toByte
    data(1) = ((x >> 8).toByte & 0xff).toByte
    data(2) = ((x >> 16).toByte & 0xff).toByte
    data(3) = ((x >> 24).toByte & 0xff).toByte
    data
  }

  private def littleEndian(x: Long): Array[Byte] = {
    val data = new Array[Byte](8)
    data(0) = ((x >> 0).toByte & 0xff).toByte
    data(1) = ((x >> 8).toByte & 0xff).toByte
    data(2) = ((x >> 16).toByte & 0xff).toByte
    data(3) = ((x >> 24).toByte & 0xff).toByte
    data(4) = ((x >> 32).toByte & 0xff).toByte
    data(5) = ((x >> 40).toByte & 0xff).toByte
    data(6) = ((x >> 48).toByte & 0xff).toByte
    data(7) = ((x >> 56).toByte & 0xff).toByte
    data
  }
}
