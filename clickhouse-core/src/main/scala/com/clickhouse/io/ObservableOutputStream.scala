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

package com.clickhouse.io

import com.clickhouse.Utils
import java.io.OutputStream
import java.util.concurrent.atomic.LongAdder

class ObservableOutputStream(
  out: OutputStream,
  writeBytesCounter: Option[LongAdder] = None,
  totalWriteByteCounter: Option[LongAdder] = None,
  writeTimeCounter: Option[LongAdder] = None,
  totalWriteTimeCounter: Option[LongAdder] = None
) extends OutputStream {

  override def write(b: Int): Unit = {
    val (_, duration) = Utils.timeTakenMs(out.write(b))
    writeBytesCounter.foreach(_.add(1L))
    totalWriteByteCounter.foreach(_.add(1L))
    writeTimeCounter.foreach(_.add(duration))
    totalWriteTimeCounter.foreach(_.add(duration))
  }

  override def write(b: Array[Byte]): Unit = {
    val (_, duration) = Utils.timeTakenMs(out.write(b))
    writeBytesCounter.foreach(_.add(b.length))
    totalWriteByteCounter.foreach(_.add(b.length))
    writeTimeCounter.foreach(_.add(duration))
    totalWriteTimeCounter.foreach(_.add(duration))
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val (_, duration) = Utils.timeTakenMs(out.write(b, off, len))
    writeBytesCounter.foreach(_.add(len))
    totalWriteByteCounter.foreach(_.add(len))
    writeTimeCounter.foreach(_.add(duration))
    totalWriteTimeCounter.foreach(_.add(duration))
  }

  override def flush(): Unit = {
    val (_, duration) = Utils.timeTakenMs(out.flush())
    writeTimeCounter.foreach(_.add(duration))
    totalWriteTimeCounter.foreach(_.add(duration))
  }

  override def close(): Unit = {
    val (_, duration) = Utils.timeTakenMs(out.close())
    writeTimeCounter.foreach(_.add(duration))
    totalWriteTimeCounter.foreach(_.add(duration))
  }
}
