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

class BatchPayloadSuite extends AnyFunSuite {

  private def readAll(is: java.io.InputStream): Array[Byte] = {
    val buf = new java.io.ByteArrayOutputStream()
    val tmp = new Array[Byte](4096)
    var n = is.read(tmp)
    while (n != -1) { buf.write(tmp, 0, n); n = is.read(tmp) }
    buf.toByteArray
  }

  test("BytesPayload reports its size and yields a fresh stream each call") {
    val bytes = Array[Byte](1, 2, 3, 4, 5)
    val p = new BytesPayload(bytes)
    assert(p.sizeBytes == 5)
    val s1 = p.asInputStream
    assert(s1.read() == 1)
    val s2 = p.asInputStream
    assert(s2.read() == 1)
    assert(readAll(p.asInputStream).toSeq == bytes.toSeq)
  }

  test("BatchHandle exposes a future that completes from its promise") {
    val h = BatchHandle(ShardKey("q", "db", "t", None), "id-1", 10L, new BytesPayload(Array[Byte](9)))
    assert(!h.future.isCompleted)
    h.promise.success(())
    assert(h.future.isCompleted)
  }
}
