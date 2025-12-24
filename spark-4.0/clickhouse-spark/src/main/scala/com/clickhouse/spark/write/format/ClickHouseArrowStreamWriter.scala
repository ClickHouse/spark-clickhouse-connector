/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under th e License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clickhouse.spark.write.format

import com.clickhouse.spark.write.{ClickHouseWriter, WriteJobDescription}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.SparkUtils
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.{StringType, StructType, VariantType}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class ClickHouseArrowStreamWriter(writeJob: WriteJobDescription) extends ClickHouseWriter(writeJob) {

  override def format: String = "ArrowStream"

  private val originalDataSchema: StructType = StructType(
    writeJob.dataSetSchema.map { field =>
      writeJob.tableSchema.find(_.name == field.name) match {
        case Some(tableField) if !tableField.nullable && field.nullable => field.copy(nullable = false)
        case _ => field
      }
    }
  )

  // TODO: Remove this workaround once ClickHouse supports reading variant/json from Arrow format.
  // This includes:
  // - The revisedDataSchema override that converts VariantType to StringType
  // - The variantFieldIndices tracking
  // - The variantToJsonString helper method
  // - The convertVariantToJson method
  // - The convertVariantToJson call in writeRow
  // - The originalDataSchema field (if only used for this workaround)

  // Convert VariantType fields to StringType for Arrow format (workaround for Arrow not supporting VariantType)
  override protected val revisedDataSchema: StructType =
    StructType(
      originalDataSchema.fields.map { field =>
        if (field.dataType == VariantType) {
          field.copy(dataType = StringType)
        } else {
          field
        }
      }
    )

  private val variantFieldIndices: Set[Int] =
    originalDataSchema.fields.zipWithIndex
      .filter(_._1.dataType == VariantType)
      .map(_._2)
      .toSet

  private def variantToJsonString(variantVal: VariantVal): UTF8String = {
    val variant = new org.apache.spark.types.variant.Variant(variantVal.getValue, variantVal.getMetadata)
    val jsonStr = variant.toJson(java.time.ZoneId.of("UTC"))
    UTF8String.fromString(jsonStr)
  }

  // Convert InternalRow: replace VariantType values with JSON strings
  private def convertVariantToJson(record: InternalRow): InternalRow =
    if (variantFieldIndices.isEmpty) {
      record
    } else {
      val values = Array.tabulate(record.numFields) { i =>
        if (variantFieldIndices.contains(i)) {
          if (record.isNullAt(i)) {
            null
          } else {
            val variantVal = record.get(i, VariantType).asInstanceOf[VariantVal]
            variantToJsonString(variantVal)
          }
        } else {
          record.get(i, originalDataSchema.fields(i).dataType)
        }
      }
      InternalRow.fromSeq(values.toIndexedSeq)
    }

  val allocator: BufferAllocator = SparkUtils.spawnArrowAllocator("writer for ClickHouse")
  val arrowSchema: Schema = SparkUtils.toArrowSchema(revisedDataSchema, writeJob.tz.getId)
  val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
  val arrowWriter: ArrowWriter = ArrowWriter.create(root)

  override def writeRow(record: InternalRow): Unit = {
    // TODO: Remove convertVariantToJson call once ClickHouse supports reading variant/json from Arrow format
    val convertedRecord = convertVariantToJson(record)
    arrowWriter.write(convertedRecord)
  }

  override def doSerialize(): Array[Byte] = {
    arrowWriter.finish()
    val arrowStreamWriter = new ArrowStreamWriter(root, null, output)
    arrowStreamWriter.writeBatch()
    arrowStreamWriter.end()
    output.flush()
    output.close()
    serializedBuffer.toByteArray
  }

  override def reset(): Unit = {
    super.reset()
    arrowWriter.reset()
  }

  override def close(): Unit = {
    root.close()
    allocator.close()
    super.close()
  }
}
