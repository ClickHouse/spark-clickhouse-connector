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

package com.clickhouse.spark.write.coalesce

import org.scalatest.funsuite.AnyFunSuite

class ArrowStreamAssemblerSuite extends AnyFunSuite {

  private def payload(bytes: Int*): BytesPayload = new BytesPayload(bytes.map(_.toByte).toArray)

  private def readAll(is: java.io.InputStream): Array[Byte] = {
    val buf = new java.io.ByteArrayOutputStream()
    val tmp = new Array[Byte](4096)
    var n = is.read(tmp)
    while (n != -1) { buf.write(tmp, 0, n); n = is.read(tmp) }
    buf.toByteArray
  }

  test("lazyStream concatenates header, each payload, then the 8-byte EOS marker") {
    val header = Array[Byte](10, 11, 12)
    val group = Seq(
      BatchHandle(ShardKey("q", "d", "t", None), "1", 1L, payload(1, 2)),
      BatchHandle(ShardKey("q", "d", "t", None), "2", 1L, payload(3, 4, 5))
    )
    val out = readAll(ArrowStreamAssembler.lazyStream(header, group))
    val expected =
      Array[Byte](10, 11, 12) ++ Array[Byte](1, 2) ++ Array[Byte](3, 4, 5) ++
        ArrowStreamAssembler.EOS
    assert(out.toSeq == expected.toSeq)
  }

  test("EOS is the IPC continuation token followed by a zero length (8 bytes)") {
    assert(ArrowStreamAssembler.EOS.toSeq == Seq[Byte](-1, -1, -1, -1, 0, 0, 0, 0))
  }

  test("lazyStream can be rebuilt and re-read (retry-safety)") {
    val header = Array[Byte](7)
    val group = Seq(BatchHandle(ShardKey("q", "d", "t", None), "1", 1L, payload(8, 9)))
    val first = readAll(ArrowStreamAssembler.lazyStream(header, group))
    val second = readAll(ArrowStreamAssembler.lazyStream(header, group))
    assert(first.toSeq == second.toSeq)
  }
}
