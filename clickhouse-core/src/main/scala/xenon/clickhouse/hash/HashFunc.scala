package xenon.clickhouse.hash

import scala.reflect.ClassTag

abstract class HashFunc[T: ClassTag] {
  def applyHash(input: Array[Byte]): T
  def combineHashes(h1: T, h2: T): T

  final def executeAny(input: Any): T =
    input match {
      case input: String => applyHash(input.getBytes)
      case _ => throw new IllegalArgumentException(s"Unsupported input type: ${input.getClass}")
    }
  final def apply(input: Array[Any]): T = input.map(executeAny).reduce(combineHashes)
}
