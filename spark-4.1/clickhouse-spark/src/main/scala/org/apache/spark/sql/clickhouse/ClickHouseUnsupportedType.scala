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

import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.sql.types._
import org.json4s.JValue

/**
 * Placeholder user class for [[ClickHouseUnsupportedType]]. Never instantiated - values of
 * unsupported columns are never materialized; reads and writes fail before reaching them.
 */
@SQLUserDefinedType(udt = classOf[ClickHouseUnsupportedType])
sealed class ClickHouseUnsupportedValue private ()

/**
 * Placeholder Spark type for a ClickHouse column whose type has no Spark mapping
 * (e.g. `AggregateFunction`, `Point`). Keeping the column in the Spark table schema -
 * instead of silently dropping it - lets queries over the supported columns run, while
 * any read or write that actually touches the column (explicitly or via `SELECT *`)
 * fails with an error naming the column. The original ClickHouse type is kept in the
 * field metadata under [[ClickHouseUnsupportedType.CLICKHOUSE_TYPE_METADATA_KEY]].
 */
class ClickHouseUnsupportedType extends UserDefinedType[ClickHouseUnsupportedValue] {

  override def sqlType: DataType = NullType

  override def userClass: Class[ClickHouseUnsupportedValue] = classOf[ClickHouseUnsupportedValue]

  override def serialize(obj: ClickHouseUnsupportedValue): Any =
    throw CHClientException("Can not write a value of an unsupported ClickHouse type")

  override def deserialize(datum: Any): ClickHouseUnsupportedValue =
    throw CHClientException("Can not read a value of an unsupported ClickHouse type")

  override def typeName: String = "unsupported"

  override def catalogString: String = "unsupported"

  override def sql: String = "UNSUPPORTED"

  override def toString: String = "unsupported"

  // Non-JVM clients (PySpark, Spark Connect) parse the schema JSON themselves and can not
  // resolve a JVM-only UDT class, so serialize as the underlying SQL type (`void`). The
  // in-memory schema keeps this UDT - the read/write guards never consume a JSON round-trip -
  // while such clients see a plain `void` column plus the `clickhouse.type` field metadata.
  override private[sql] def jsonValue: JValue = sqlType.jsonValue
}

object ClickHouseUnsupportedType {

  /** Dedicated flag marking a field as an unsupported placeholder - the sole test for [[isUnsupportedColumn]]. */
  val UNSUPPORTED_MARKER_KEY = "clickhouse.unsupported"

  /** Payload holding the original ClickHouse type string of a placeholder column. */
  val CLICKHOUSE_TYPE_METADATA_KEY = "clickhouse.type"

  val INSTANCE = new ClickHouseUnsupportedType

  def field(name: String, chType: String): StructField =
    StructField(
      name,
      INSTANCE,
      nullable = true,
      metadata = new MetadataBuilder()
        .putBoolean(UNSUPPORTED_MARKER_KEY, value = true)
        .putString(CLICKHOUSE_TYPE_METADATA_KEY, chType)
        .build()
    )

  // The in-memory UDT, or its round-tripped form flagged by the dedicated marker (not the type payload).
  def isUnsupportedColumn(field: StructField): Boolean =
    field.dataType.isInstanceOf[ClickHouseUnsupportedType] ||
      (field.metadata.contains(UNSUPPORTED_MARKER_KEY) && field.metadata.getBoolean(UNSUPPORTED_MARKER_KEY))

  /** Returns the `(name, original ClickHouse type)` of the unsupported columns in the schema. */
  def unsupportedColumns(schema: StructType): Seq[(String, String)] =
    schema.fields.toSeq.collect {
      case f if isUnsupportedColumn(f) =>
        val chType =
          if (f.metadata.contains(CLICKHOUSE_TYPE_METADATA_KEY)) f.metadata.getString(CLICKHOUSE_TYPE_METADATA_KEY)
          else "unknown"
        f.name -> chType
    }
}
