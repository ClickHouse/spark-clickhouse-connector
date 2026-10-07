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

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader
import com.clickhouse.spark.format.RowBinaryInput.Unreadable
import com.clickhouse.spark.format.RowBinaryTestData._
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, InputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.Arrays
import scala.util.Random

class RowBinaryInputSuite extends AnyFunSuite {

  private def input(bytes: Array[Byte], chunkSize: Int = RowBinaryInput.ChunkSize, trickle: Int = 0): RowBinaryInput = {
    val in = new ByteArrayInputStream(bytes)
    new RowBinaryInput(if (trickle > 0) new Trickle(in, trickle) else in, chunkSize)
  }

  private def readAll(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](7)
    var n = in.read(buf, 0, buf.length)
    while (n >= 0) {
      out.write(buf, 0, n)
      n = in.read(buf, 0, buf.length)
    }
    out.toByteArray
  }

  test("little-endian reads match client-v2 across every buffer boundary") {
    val random = new Random(42)
    val bytes = Array.fill(4096)(random.nextInt(256).toByte)
    val widths = Iterator.continually(Seq(1, 2, 4, 8)).flatten.take(bytes.length / 4).toSeq
    for (chunkSize <- Seq(1, 2, 3, 5, 7, 8, 13, 64); trickle <- Seq(0, 1, 3)) {
      val in = input(bytes, chunkSize, trickle)
      val expected = new ByteArrayInputStream(bytes)
      val scratch = new Array[Byte](8)
      widths.foreach {
        case 1 => assert(in.readByte() === BinaryStreamReader.readByteOrEOF(expected).toByte)
        case 2 => assert(in.readShortLE() === BinaryStreamReader.readShortLE(expected, scratch))
        case 4 => assert(in.readIntLE() === BinaryStreamReader.readIntLE(expected, scratch))
        case 8 => assert(in.readLongLE() === BinaryStreamReader.readLongLE(expected, scratch))
      }
    }
  }

  test("varints match client-v2, including ones that overflow an Int") {
    def leb128(value: Long): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      var v = value
      do {
        val b = (v & 0x7f).toInt
        v >>>= 7
        out.write(if (v != 0) b | 0x80 else b)
      } while (v != 0)
      out.toByteArray
    }
    val values = Seq(
      0L,
      1L,
      127L,
      128L,
      255L,
      16383L,
      16384L,
      1L << 21,
      1L << 28,
      Int.MaxValue.toLong,
      1L << 31,
      (1L << 32) - 1,
      1L << 35,
      Long.MaxValue,
      -1L
    )
    val random = new Random(7)
    val garbage = Seq(Array.fill(10)(0xff.toByte), Array.fill(12)(0x80.toByte)) ++
      Seq.fill(200)(Array.fill(random.nextInt(12) + 1)(random.nextInt(256).toByte) :+ 0.toByte)
    for (encoded <- values.map(leb128) ++ garbage; chunkSize <- Seq(1, 4, 64)) {
      val bytes = encoded ++ Array[Byte](42)
      val expected = new ByteArrayInputStream(bytes)
      val in = input(bytes, chunkSize)
      assert(in.readVarInt() === BinaryStreamReader.readVarInt(expected), encoded.mkString(","))
      // both consumed the same bytes
      assert(in.readByte() === expected.read().toByte)
    }
  }

  test("strings match client-v2, including invalid UTF-8") {
    val strings: Seq[Array[Byte]] = Seq(
      Array.emptyByteArray,
      "ascii".getBytes(StandardCharsets.UTF_8),
      "héllo wörld ✓ 𝄞".getBytes(StandardCharsets.UTF_8),
      Array(0xff, 0xfe, 0x80, 0xc3).map(_.toByte),
      Array(0xed, 0xa0, 0x80).map(_.toByte), // a UTF-16 surrogate, invalid in UTF-8
      Array.fill(3000)('x'.toByte)
    )
    val bytes = strings.map(s => stringBytes(s).bytes).reduce(_ ++ _)
    val expected = new BinaryStreamReaderOver(bytes)
    val in = input(bytes, chunkSize = 16, trickle = 5)
    strings.foreach(_ => assert(in.readString() === expected.readString()))
    assert(!in.hasMore)
  }

  test("a negative string length is unreadable") {
    val in = input(Array(0xff, 0xff, 0xff, 0xff, 0x0f).map(_.toByte))
    assertThrows[Unreadable.type](in.readString())
  }

  test("end of stream") {
    val in = input(Array[Byte](1, 2, 3))
    assert(in.hasMore)
    assertThrows[Unreadable.type](in.readIntLE())
    assert(in.readShortLE() === 0x0201)
    assert(in.readByte() === 3)
    assert(!in.hasMore)
    assert(!in.failed)
    assertThrows[Unreadable.type](in.readByte())
  }

  test("a failing stream stops the reader and fails the replay at the same byte") {
    val failure = new IOException("connection reset")
    val in = new RowBinaryInput(new FailingStream(Array[Byte](1, 2, 3, 4, 5), failure), chunkSize = 2)
    in.readByte()
    in.mark()
    assertThrows[Unreadable.type](in.readLongLE())
    assert(in.failed)
    assert(in.readIntLE() === 0x05040302)
    assert(!in.hasMore)
    val replay = in.replay(Array[Byte](9))
    assert((1 to 5).map(_ => replay.read()) === Seq(9, 2, 3, 4, 5))
    assert(intercept[IOException](replay.read()) eq failure)
  }

  test("replay serves the prefix, the bytes since the mark and the rest of the stream") {
    val bytes = Array.tabulate[Byte](1000)(_.toByte)
    for (chunkSize <- Seq(1, 8, 100, 4096)) {
      val in = input(bytes, chunkSize, trickle = 3)
      in.take(10)
      in.mark()
      in.take(500)
      assert(readAll(in.replay(Array[Byte](-1, -2))) === Array[Byte](-1, -2) ++ bytes.drop(10))
    }
  }

  test("replay after the end of the stream ends where the reader's input ended") {
    val in = input(Array[Byte](1, 2, 3))
    in.mark()
    assertThrows[Unreadable.type](in.readIntLE())
    assert(readAll(in.replay(Array.emptyByteArray)) === Array[Byte](1, 2, 3))
  }

  test("the buffer keeps a long marked row and shrinks back after it") {
    val bytes = Array.tabulate[Byte](300)(_.toByte)
    val in = input(bytes, chunkSize = 16)
    in.mark()
    val offset = in.take(200)
    assert(in.buffer.slice(offset, offset + 200) === bytes.take(200))
    assert(in.buffer.length === 256)
    in.take(56)
    in.mark()
    in.take(10)
    assert(in.buffer.length === 16)
    assert(readAll(in.replay(Array.emptyByteArray)) === bytes.drop(256))
  }

  test("isAscii") {
    val in = input(Array.fill(40)('a'.toByte))
    val offset = in.take(40)
    assert(in.isAscii(offset, 40))
    assert(in.isAscii(offset, 0))
    for (at <- 0 until 40) {
      in.buffer(offset + at) = 0xc3.toByte
      assert(!in.isAscii(offset, 40))
      assert(in.isAscii(offset, at))
      assert(in.isAscii(offset + at + 1, 40 - at - 1))
      in.buffer(offset + at) = 'a'.toByte
    }
  }

  test("from client-v2's LZ4 stream it reads block by block, so a failing block loses no bytes") {
    val bytes = Array.tabulate[Byte](5000)(i => (i * 31).toByte)
    val compressed = lz4(bytes, blockSize = 700)
    val failure = new IOException("connection reset")
    for (cut <- 0 to compressed.length) {
      // client-v2 reads values of a few bytes, so it gets every byte of the blocks that arrived whole
      val clientBytes = new ByteArrayOutputStream()
      val client = unlz4(new FailingStream(Arrays.copyOf(compressed, cut), failure))
      val clientFailure =
        try {
          Iterator.continually(client.read()).takeWhile(_ >= 0).foreach(clientBytes.write)
          None
        } catch {
          case e: IOException => Some(e)
        }
      val in = new RowBinaryInput(unlz4(new FailingStream(Arrays.copyOf(compressed, cut), failure)), chunkSize = 1024)
      val read = new ByteArrayOutputStream()
      while (in.hasMore) read.write(in.readByte())
      assert(read.toByteArray === clientBytes.toByteArray, s"cut at $cut")
      assert(in.failed === clientFailure.isDefined, s"cut at $cut")
    }
  }

  test("from client-v2's LZ4 stream it doesn't wait for a block before it needs its bytes") {
    val compressed = lz4(Array.fill[Byte](3000)(7), blockSize = 1000)
    val firstBlock = 16 + ByteBuffer.wrap(compressed, 17, 4).order(ByteOrder.LITTLE_ENDIAN).getInt
    // the second block hasn't arrived yet
    val in = new RowBinaryInput(unlz4(new FailingStream(compressed.take(firstBlock), new IOException("would block"))))
    in.take(1000)
    assert(!in.failed)
    assert(!in.hasMore && in.failed)
  }

  test("readHeader") {
    val columns = Seq("id" -> "UInt64", "name" -> "LowCardinality(Nullable(String))")
    val bytes = header(columns) ++ Array[Byte](1, 2, 3)
    val parsed = input(bytes, chunkSize = 3).readHeader(2).get
    assert(parsed.map(c => c.getColumnName -> c.getOriginalTypeName).toSeq === columns)

    assert(input(bytes).readHeader(3).isEmpty)
    assert(input(bytes.take(9)).readHeader(2).isEmpty)
    assert(input(header(Seq("x" -> "NotAType(1"))).readHeader(1).isEmpty)
    assert(input(Array.emptyByteArray).readHeader(1).isEmpty)
  }

  // client-v2's own string reader over the same bytes
  final private class BinaryStreamReaderOver(bytes: Array[Byte]) {
    private val in = new ByteArrayInputStream(bytes)
    def readString(): String = BinaryStreamReader.readString(in)
  }
}
