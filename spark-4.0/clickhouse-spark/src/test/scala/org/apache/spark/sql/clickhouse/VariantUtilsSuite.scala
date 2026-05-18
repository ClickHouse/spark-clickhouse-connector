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

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}
import org.scalatest.funsuite.AnyFunSuite

class VariantUtilsSuite extends AnyFunSuite {

  private val NestingDepth = 100

  private def makeVariant(json: String): VariantVal = {
    val parser = new JsonFactory().createParser(json)
    parser.nextToken()
    val v = VariantBuilder.parseJson(parser, false)
    new VariantVal(v.getValue, v.getMetadata)
  }

  // Build a DataType of nesting depth `depth`, cycling Struct/Array/Map as wrappers, with a
  // single VariantType leaf. depth=0 returns VariantType itself.
  private def nestedType(depth: Int): DataType =
    if (depth == 0) VariantType
    else (depth % 3) match {
      case 0 => StructType(Seq(StructField("f", nestedType(depth - 1))))
      case 1 => ArrayType(nestedType(depth - 1), containsNull = false)
      case 2 => MapType(StringType, nestedType(depth - 1), valueContainsNull = false)
    }

  // Build a value tree matching `dt`, with a single chain that terminates at `leaf`.
  private def nestedValue(dt: DataType, leaf: VariantVal): Any = dt match {
    case VariantType => leaf
    case st: StructType =>
      new GenericInternalRow(Array[Any](nestedValue(st.fields.head.dataType, leaf)))
    case ArrayType(inner, _) =>
      new GenericArrayData(Array[Any](nestedValue(inner, leaf)))
    case MapType(_, valueType, _) =>
      new ArrayBasedMapData(
        new GenericArrayData(Array[Any](UTF8String.fromString("k"))),
        new GenericArrayData(Array[Any](nestedValue(valueType, leaf)))
      )
    case other => fail(s"unexpected DataType: $other")
  }

  // Walk into a single-chain DataType `depth` levels and return the leaf type.
  private def walkType(dt: DataType, depth: Int): DataType = {
    var cur: DataType = dt
    for (_ <- 0 until depth) cur = cur match {
      case st: StructType => st.fields.head.dataType
      case ArrayType(inner, _) => inner
      case MapType(_, valueType, _) => valueType
      case other => fail(s"cannot walk into: $other")
    }
    cur
  }

  // Walk into a single-chain value, parallel to walkType, returning the leaf value.
  private def walkValue(value: Any, dt: DataType, depth: Int): Any = {
    var curV: Any = value
    var curT: DataType = dt
    for (_ <- 0 until depth) (curT, curV) match {
      case (st: StructType, row: InternalRow) =>
        val inner = st.fields.head.dataType
        curV = if (row.isNullAt(0)) null else row.get(0, inner)
        curT = inner
      case (ArrayType(inner, _), arr: ArrayData) =>
        curV = if (arr.isNullAt(0)) null else arr.get(0, inner)
        curT = inner
      case (MapType(_, valueType, _), map: MapData) =>
        val values = map.valueArray()
        curV = if (values.isNullAt(0)) null else values.get(0, valueType)
        curT = valueType
      case (t, v) => fail(s"cannot walk into type=$t value=$v")
    }
    curV
  }

  test(s"replaceVariantInType / containsVariant survive depth=$NestingDepth nested Struct/Array/Map") {
    val dt = nestedType(NestingDepth)
    assert(VariantUtils.containsVariant(dt))

    val rewritten = VariantUtils.replaceVariantInType(dt)
    assert(!VariantUtils.containsVariant(rewritten))
    assert(walkType(rewritten, NestingDepth) === StringType)
  }

  test(s"jsonifyVariantsInRow survives depth=$NestingDepth nested Struct/Array/Map") {
    // Outer wrapper is the row's StructType, so the inner chain has depth-1 wrappers + Variant leaf.
    val innerDepth = NestingDepth - 1
    val innerType = nestedType(innerDepth)
    val schema = StructType(Seq(StructField("root", innerType)))

    val variantLeaf = makeVariant("""{"k":1}""")
    val row = new GenericInternalRow(Array[Any](nestedValue(innerType, variantLeaf)))

    val converted = VariantUtils.jsonifyVariantsInRow(row, schema)

    val rewrittenInner = VariantUtils.replaceVariantInType(innerType)
    val leafValue = walkValue(converted.get(0, rewrittenInner), rewrittenInner, innerDepth)
    assert(leafValue.isInstanceOf[UTF8String], s"leaf was ${leafValue.getClass}")
    assert(leafValue.toString.contains("\"k\""))
  }
}
