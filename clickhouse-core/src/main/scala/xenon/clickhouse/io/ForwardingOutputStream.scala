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

import java.io.OutputStream

class ForwardingOutputStream(@volatile var delegate: OutputStream = null) extends OutputStream {

  def updateDelegate(delegate: OutputStream): Unit = this.delegate = delegate

  override def write(b: Int): Unit = delegate.write(b)

  override def write(b: Array[Byte]): Unit = delegate.write(b)

  override def write(b: Array[Byte], off: Int, len: Int): Unit = delegate.write(b, off, len)

  override def flush(): Unit = delegate.flush()

  override def close(): Unit = delegate.close()
}
