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
import org.apache.spark.sql.clickhouse.{SparkUtils, VariantUtils}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType

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

  // Workaround: Apache Arrow IPC has no native VariantType encoding, so we ship Variants as
  // JSON strings. `revisedDataSchema` rewrites every VariantType (at any nesting depth) to
  // StringType for the Arrow schema, and `writeRow` uses `VariantUtils.jsonifyVariantsInRow`
  // to convert each row's Variant values to JSON UTF8Strings.
  // TODO: Remove this workaround once ClickHouse supports reading variant/json from Arrow format.
  override protected val revisedDataSchema: StructType =
    StructType(originalDataSchema.fields.map(f => f.copy(dataType = VariantUtils.replaceVariantInType(f.dataType))))

  val allocator: BufferAllocator = SparkUtils.spawnArrowAllocator("writer for ClickHouse")
  val arrowSchema: Schema = SparkUtils.toArrowSchema(revisedDataSchema, writeJob.tz.getId)
  val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
  val arrowWriter: ArrowWriter = ArrowWriter.create(root)

  override def writeRow(record: InternalRow): Unit =
    arrowWriter.write(VariantUtils.jsonifyVariantsInRow(record, originalDataSchema))

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
