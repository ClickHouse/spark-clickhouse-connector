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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecializedGetters}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructType, VariantType}
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

import java.time.ZoneId

object VariantUtils {

  private val UTC: ZoneId = ZoneId.of("UTC")

  /**
   * Recursively rewrite a Spark DataType, replacing every `VariantType` (top-level or nested
   * inside `ArrayType` / `MapType` / `StructType`) with `StringType`. Other types pass through
   * unchanged.
   */
  def replaceVariantInType(dt: DataType): DataType = dt match {
    case VariantType => StringType
    case ArrayType(elem, n) => ArrayType(replaceVariantInType(elem), n)
    case MapType(k, v, n) => MapType(replaceVariantInType(k), replaceVariantInType(v), n)
    case st: StructType => StructType(st.fields.map(f => f.copy(dataType = replaceVariantInType(f.dataType))))
    case other => other
  }

  /** True iff `dt` contains a `VariantType` at any depth. */
  def containsVariant(dt: DataType): Boolean = dt match {
    case VariantType => true
    case ArrayType(e, _) => containsVariant(e)
    case MapType(k, v, _) => containsVariant(k) || containsVariant(v)
    case st: StructType => st.fields.exists(f => containsVariant(f.dataType))
    case _ => false
  }

  /** Convert a single `VariantVal` to a UTF-8 JSON string. */
  def variantToJsonString(variantVal: VariantVal): UTF8String = {
    val variant = new Variant(variantVal.getValue, variantVal.getMetadata)
    UTF8String.fromString(variant.toJson(UTC))
  }

  /**
   * Recursively rewrite `value`, converting every `VariantVal` at any depth into a JSON
   * UTF8String. `dt` is the *original* (pre-rewrite) Spark type for `value`. Subtrees that
   * contain no Variant are returned as-is, so the cost is paid only along Variant-bearing paths.
   */
  def convertVariantsToJson(value: Any, dt: DataType): Any = (value, dt) match {
    case (null, _) => null
    case (v: VariantVal, VariantType) => variantToJsonString(v)
    case (arr: ArrayData, ArrayType(elem, _)) if containsVariant(elem) =>
      new GenericArrayData(Array.tabulate[Any](arr.numElements())(convertAt(arr, _, elem)))
    case (mapData: MapData, MapType(keyType, valueType, _)) if containsVariant(keyType) || containsVariant(valueType) =>
      val keys = mapData.keyArray()
      val values = mapData.valueArray()
      val newKeys = Array.tabulate[Any](keys.numElements())(convertAt(keys, _, keyType))
      val newValues = Array.tabulate[Any](values.numElements())(convertAt(values, _, valueType))
      new ArrayBasedMapData(new GenericArrayData(newKeys), new GenericArrayData(newValues))
    case (row: InternalRow, st: StructType) if containsVariant(st) =>
      val out = Array.tabulate[Any](st.fields.length)(i => convertAt(row, i, st.fields(i).dataType))
      new GenericInternalRow(out)
    case (other, _) => other
  }

  private def convertAt(source: SpecializedGetters, i: Int, dt: DataType): Any =
    if (source.isNullAt(i)) null else convertVariantsToJson(source.get(i, dt), dt)

  /**
   * Build a new `InternalRow` where every Variant value (at any depth) has been replaced with
   * its JSON UTF8String form. Returns the original `record` unchanged when `schema` contains
   * no Variants.
   */
  def jsonifyVariantsInRow(record: InternalRow, schema: StructType): InternalRow =
    if (!schema.fields.exists(f => containsVariant(f.dataType))) record
    else {
      val out = Array.tabulate[Any](record.numFields)(i => convertAt(record, i, schema.fields(i).dataType))
      new GenericInternalRow(out)
    }
}
