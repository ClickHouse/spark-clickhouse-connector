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

import com.clickhouse.data.ClickHouseDataType._
import com.clickhouse.data.{ClickHouseColumn, ClickHouseDataType}
import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.READ_FIXED_STRING_AS

import scala.collection.JavaConverters._

object SchemaUtils extends SQLConfHelper {

  def fromClickHouseType(chColumn: ClickHouseColumn): (DataType, Boolean) = {
    val catalystType = chColumn.getDataType match {
      case Nothing => NullType
      case Bool => BooleanType
      case String | JSON | UUID | Enum8 | Enum16 | IPv4 | IPv6 => StringType
      case FixedString =>
        conf.getConf(READ_FIXED_STRING_AS) match {
          case "binary" => BinaryType
          case "string" => StringType
          case unsupported => throw CHClientException(s"Unsupported fixed string read format mapping: $unsupported")
        }
      case Int8 => ByteType
      case UInt8 =>
        // Check if this UInt8 is actually a Bool (ClickHouse stores Bool as UInt8)
        if (chColumn.getOriginalTypeName.toLowerCase.contains("bool")) BooleanType
        else ShortType
      case Int16 => ShortType
      case UInt16 | Int32 => IntegerType
      case UInt32 | Int64 => LongType
      case UInt64 => DecimalType(20, 0)
      case Int128 | UInt128 | Int256 | UInt256 => DecimalType(38, 0)
      case Float32 => FloatType
      case Float64 => DoubleType
      case Date | Date32 => DateType
      case DateTime | DateTime32 | DateTime64 => TimestampType
      case ClickHouseDataType.Decimal if chColumn.getScale <= 38 =>
        DecimalType(chColumn.getPrecision, chColumn.getScale)
      case Decimal32 => DecimalType(9, chColumn.getScale)
      case Decimal64 => DecimalType(18, chColumn.getScale)
      case Decimal128 => DecimalType(38, chColumn.getScale)
      case IntervalYear => YearMonthIntervalType(YearMonthIntervalType.YEAR)
      case IntervalMonth => YearMonthIntervalType(YearMonthIntervalType.MONTH)
      case IntervalDay => DayTimeIntervalType(DayTimeIntervalType.DAY)
      case IntervalHour => DayTimeIntervalType(DayTimeIntervalType.HOUR)
      case IntervalMinute => DayTimeIntervalType(DayTimeIntervalType.MINUTE)
      case IntervalSecond => DayTimeIntervalType(DayTimeIntervalType.SECOND)
      case Array =>
        val elementChCols = chColumn.getNestedColumns
        assert(elementChCols.size == 1)
        val (elementType, elementNullable) = fromClickHouseType(elementChCols.get(0))
        ArrayType(elementType, elementNullable)
      case Map =>
        val kvChCols = chColumn.getNestedColumns
        assert(kvChCols.size == 2)
        val (keyChType, valueChType) = (kvChCols.get(0), kvChCols.get(1))
        val (keyType, keyNullable) = fromClickHouseType(keyChType)
        require(
          !keyNullable,
          s"Illegal type: ${keyChType.getOriginalTypeName}, the key type of Map should not be nullable"
        )
        val (valueType, valueNullable) = fromClickHouseType(valueChType)
        MapType(keyType, valueType, valueNullable)
      case Tuple =>
        val nestedCols = chColumn.getNestedColumns
        val fields = nestedCols.asScala.zipWithIndex.map { case (col, idx) =>
          val (fieldType, fieldNullable) = fromClickHouseType(col)
          // Use Spark convention for unnamed tuple fields: _1, _2, etc.
          val fieldName = if (col.getColumnName.isEmpty) s"_${idx + 1}" else col.getColumnName
          StructField(fieldName, fieldType, fieldNullable)
        }.toArray
        StructType(fields)
      case Object | Nested | Point | Polygon | MultiPolygon | Ring | IntervalQuarter | IntervalWeek |
          Decimal256 | AggregateFunction | SimpleAggregateFunction =>
        throw CHClientException(s"Unsupported type: ${chColumn.getOriginalTypeName}")
    }
    (catalystType, chColumn.isNullable)
  }

  def toClickHouseType(catalystType: DataType, nullable: Boolean): String =
    catalystType match {
      case BooleanType => maybeNullable("Bool", nullable)
      case ByteType => maybeNullable("Int8", nullable)
      case ShortType => maybeNullable("Int16", nullable)
      case IntegerType => maybeNullable("Int32", nullable)
      case LongType => maybeNullable("Int64", nullable)
      case FloatType => maybeNullable("Float32", nullable)
      case DoubleType => maybeNullable("Float64", nullable)
      case StringType => maybeNullable("String", nullable)
      case VarcharType(_) => maybeNullable("String", nullable)
      case CharType(_) => maybeNullable("String", nullable) // TODO: maybe FixString?
      case DateType => maybeNullable("Date", nullable)
      case TimestampType => maybeNullable("DateTime", nullable)
      case DecimalType.Fixed(p, s) => maybeNullable(s"Decimal($p, $s)", nullable)
      case ArrayType(elemType, containsNull) => s"Array(${toClickHouseType(elemType, containsNull)})"
      // TODO currently only support String as key
      case MapType(keyType, valueType, valueContainsNull) if keyType.isInstanceOf[StringType] =>
        s"Map(${toClickHouseType(keyType, nullable = false)}, ${toClickHouseType(valueType, valueContainsNull)})"
      case struct: StructType =>
        val fieldTypes = struct.fields.map { field =>
          val fieldType = toClickHouseType(field.dataType, field.nullable)
          s"${field.name} ${fieldType}"
        }.mkString(", ")
        s"Tuple($fieldTypes)"
      case _ => throw CHClientException(s"Unsupported type: $catalystType")
    }

  def fromClickHouseSchema(chSchema: Seq[(String, String)]): StructType = {
    val structFields = chSchema.map { case (name, maybeNullableType) =>
      val chCols = ClickHouseColumn.parse(s"`$name` $maybeNullableType")
      assert(chCols.size == 1)
      val (sparkType, nullable) = fromClickHouseType(chCols.get(0))
      StructField(name, sparkType, nullable)
    }
    StructType(structFields)
  }

  def toClickHouseSchema(catalystSchema: StructType): Seq[(String, String, String)] =
    catalystSchema.fields
      .map { field =>
        val chType = toClickHouseType(field.dataType, field.nullable)
        (field.name, chType, field.getComment().map(c => s" COMMENT '$c'").getOrElse(""))
      }

  private[clickhouse] def maybeNullable(chType: String, nullable: Boolean): String =
    if (nullable) wrapNullable(chType) else chType

  private[clickhouse] def wrapNullable(chType: String): String = s"Nullable($chType)"
}
