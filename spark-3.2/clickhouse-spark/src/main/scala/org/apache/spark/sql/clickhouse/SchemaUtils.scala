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

package org.apache.spark.sql.clickhouse

import scala.util.matching.Regex

import org.apache.spark.sql.types._
import xenon.clickhouse.exception.CHClientException

object SchemaUtils {

  // format: off
  private[clickhouse] val arrayTypePattern:          Regex = """^Array\((.+)\)$""".r
  private[clickhouse] val mapTypePattern:            Regex = """^Map\((\w+),\s*(.+)\)$""".r
  private[clickhouse] val dateTypePattern:           Regex = """^Date$""".r
  private[clickhouse] val dateTimeTypePattern:       Regex = """^DateTime(64)?(\((.*)\))?$""".r
  private[clickhouse] val decimalTypePattern:        Regex = """^Decimal\((\d+),\s*(\d+)\)$""".r
  private[clickhouse] val decimalTypePattern2:       Regex = """^Decimal(32|64|128|256)\((\d+)\)$""".r
  private[clickhouse] val enumTypePattern:           Regex = """^Enum(8|16)$""".r
  private[clickhouse] val fixedStringTypePattern:    Regex = """^FixedString\((\d+)\)$""".r
  private[clickhouse] val nullableTypePattern:       Regex = """^Nullable\((.*)\)""".r
  private[clickhouse] val lowCardinalityTypePattern: Regex = """^LowCardinality\((.*)\)""".r
  // format: on

  def fromClickHouseType(chType: String): (DataType, Boolean) = {
    val (unwrappedChType, nullable) = unwrapNullable(unwrapLowCardinalityTypePattern(chType))
    val catalystType = unwrappedChType match {
      case "String" | "UUID" | fixedStringTypePattern() | enumTypePattern(_) => StringType
      case "Int8" => ByteType
      case "UInt8" | "Int16" => ShortType
      case "UInt16" | "Int32" => IntegerType
      case "UInt32" | "Int64" | "UInt64" | "IPv4" => LongType
      case "Int128" | "Int256" | "UInt256" =>
        throw CHClientException(s"unsupported type: $chType") // not support
      case "Float32" => FloatType
      case "Float64" => DoubleType
      case dateTypePattern() => DateType
      case dateTimeTypePattern(_, _, _) => TimestampType
      case decimalTypePattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case decimalTypePattern2(w, scale) => w match {
          case "32" => DecimalType(9, scale.toInt)
          case "64" => DecimalType(18, scale.toInt)
          case "128" => DecimalType(38, scale.toInt)
          case "256" => DecimalType(76, scale.toInt) // throw exception, spark support precision up to 38
        }
      case arrayTypePattern(nestedChType) =>
        val (_type, _nullable) = fromClickHouseType(nestedChType)
        ArrayType(_type, _nullable)
      case mapTypePattern(keyChType, valueChType) =>
        val (_keyType, _keyNullable) = fromClickHouseType(keyChType)
        require(!_keyNullable, s"Illegal type: $keyChType, the key type of Map should not be nullable")
        val (_valueType, _valueNullable) = fromClickHouseType(valueChType)
        MapType(_keyType, _valueType, _valueNullable)
      case _ => throw CHClientException(s"Unsupported type: $chType")
    }
    (catalystType, nullable)
  }

  def toClickHouseType(catalystType: DataType): String =
    catalystType match {
      case BooleanType => "UInt8"
      case ByteType => "Int8"
      case ShortType => "Int16"
      case IntegerType => "Int32"
      case LongType => "Int64"
      case FloatType => "Float32"
      case DoubleType => "Float64"
      case StringType => "String"
      case VarcharType(_) => "String"
      case CharType(_) => "String" // TODO: maybe FixString?
      case DateType => "Date"
      case TimestampType => "DateTime"
      case DecimalType.Fixed(p, s) => s"Decimal($p, $s)"
      case ArrayType(elemType, nullable) => s"Array(${maybeNullable(toClickHouseType(elemType), nullable)})"
      // TODO currently only support String as key
      case MapType(keyType, valueType, nullable) if keyType.isInstanceOf[StringType] =>
        s"Map(${toClickHouseType(keyType)},${maybeNullable(toClickHouseType(valueType), nullable)})"
      case _ => throw CHClientException(s"Unsupported type: $catalystType")
    }

  def fromClickHouseSchema(chSchema: Seq[(String, String)]): StructType = {
    val structFields = chSchema
      .map { case (name, maybeNullableType) =>
        val (catalyst, nullable) = fromClickHouseType(maybeNullableType)
        StructField(name, catalyst, nullable)
      }
    StructType(structFields)
  }

  def toClickHouseSchema(catalystSchema: StructType): Seq[(String, String, String)] =
    catalystSchema.fields
      .map { field =>
        val chType = toClickHouseType(field.dataType)
        (field.name, maybeNullable(chType, field.nullable), field.getComment().map(c => s" COMMENT '$c'").getOrElse(""))
      }

  private[clickhouse] def maybeNullable(chType: String, nullable: Boolean): String =
    if (nullable) wrapNullable(chType) else chType

  private[clickhouse] def wrapNullable(chType: String): String = s"Nullable($chType)"

  private[clickhouse] def unwrapNullable(maybeNullableType: String): (String, Boolean) =
    maybeNullableType match {
      case nullableTypePattern(typeName) => (typeName, true)
      case _ => (maybeNullableType, false)
    }

  private[clickhouse] def unwrapLowCardinalityTypePattern(maybeLowCardinalityType: String): String =
    maybeLowCardinalityType match {
      case lowCardinalityTypePattern(typeName) => typeName
      case _ => maybeLowCardinalityType
    }
}
