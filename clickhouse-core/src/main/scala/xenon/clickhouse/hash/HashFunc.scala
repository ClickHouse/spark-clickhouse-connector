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

package xenon.clickhouse.hash

import scala.reflect.ClassTag

abstract class HashFunc[T: ClassTag] {
  def applyHash(input: Array[Byte]): T
  def combineHashes(h1: T, h2: T): T

  final def executeAny(input: Any): T =
    input match {
      case bytes: Array[Byte] => applyHash(bytes)
      case string: String => applyHash(string.getBytes(StandardCharsets.UTF_8))
      case _ => throw new IllegalArgumentException(s"Unsupported input type: ${input.getClass}")
    }
  final def apply(input: Array[Any]): T = input.map(executeAny).reduce(combineHashes)
}
