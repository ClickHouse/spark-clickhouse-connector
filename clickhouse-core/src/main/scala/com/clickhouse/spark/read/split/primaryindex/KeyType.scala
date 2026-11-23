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

package com.clickhouse.spark.read.split.primaryindex

/** How a key column's cut values are read from the index and written back as SQL literals. */
sealed private[primaryindex] trait KeyType {
  def read(column: String): String
  def literal(value: String): String
}

private[primaryindex] object KeyType {

  case object IntegerKey extends KeyType {
    def read(column: String): String = s"toString($column)"
    def literal(value: String): String = {
      require(value.matches("-?\\d+"), s"Not an integer: $value")
      value
    }
  }

  // Date, Date32 and UUID round-trip through their string form
  case object StringFormKey extends KeyType {
    def read(column: String): String = s"toString($column)"
    def literal(value: String): String = SqlText.quote(value)
  }

  // in UTC, so a cut does not depend on the server's or the column's time zone
  case class DateTimeKey(precision: Option[Int]) extends KeyType {
    def read(column: String): String = s"toString($column, 'UTC')"
    def literal(value: String): String = precision match {
      case Some(p) => s"toDateTime64(${SqlText.quote(value)}, $p, 'UTC')"
      case None => s"toDateTime(${SqlText.quote(value)}, 'UTC')"
    }
  }

  // hex-encoded, as the JSON output does not carry arbitrary bytes
  case object BytesKey extends KeyType {
    def read(column: String): String = s"hex($column)"
    def literal(value: String): String = bytesLiteral(value)
  }

  private val IntegerType = "U?Int(?:8|16|32|64)".r
  private val DateTime64Type = "DateTime64\\((\\d+).*\\)".r
  private val DateTimeType = "DateTime(?:\\(.*\\))?".r
  private val FixedStringType = "FixedString\\(\\d+\\)".r
  private val LowCardinalityType = "LowCardinality\\((.*)\\)".r

  /** The key types ranges can be cut on. NULL and NaN fall outside every range, so Nullable and floats can't. */
  def of(clickHouseType: String): Option[KeyType] = clickHouseType match {
    case LowCardinalityType(inner) => of(inner)
    case IntegerType() => Some(IntegerKey)
    case "Date" | "Date32" | "UUID" => Some(StringFormKey)
    case DateTime64Type(precision) => Some(DateTimeKey(Some(precision.toInt)))
    case DateTimeType() => Some(DateTimeKey(None))
    case "String" | FixedStringType() => Some(BytesKey)
    case _ => None
  }

  // a string literal of hex-encoded bytes, escaping all but printable ASCII
  private def bytesLiteral(hex: String): String = {
    require(hex.length % 2 == 0 && hex.forall(Character.digit(_, 16) >= 0), s"Not a hex string: $hex")
    hex.grouped(2).map(Integer.parseInt(_, 16).toChar).map {
      case c @ ('\'' | '\\') => s"\\$c"
      case c if c >= ' ' && c <= '~' => c.toString
      case c => f"\\x${c.toInt}%02X"
    }.mkString("'", "", "'")
  }
}
