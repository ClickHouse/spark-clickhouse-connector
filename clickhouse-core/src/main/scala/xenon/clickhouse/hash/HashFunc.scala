package xenon.clickhouse.hash

abstract class HashFunc[T] {
  def applyHash(input: Array[Byte]): T
  def combineHashes(h1: T, h2: T): T

  final def executeAny(input: Any): T =
    input match {
      case input: String => applyHash(input.getBytes)
      case _ => throw new IllegalArgumentException(s"Unsupported input type: ${input.getClass}")
    }
  final def apply(input: Array[Any]): T = input.map(executeAny).reduce(combineHashes)
}
