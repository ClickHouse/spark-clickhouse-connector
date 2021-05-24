package org.apache.spark.sql

import java.io.StringWriter
import java.time.ZoneId
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.StructType

import java.nio.charset.StandardCharsets

object JsonFormatUtil {

  private val option = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss"
  )

  // inefficiently
  def row2Json(row: InternalRow, schema: StructType, tz: ZoneId): Array[Byte] = {
    val line = new StringWriter()
    val gen = new JacksonGenerator(schema, line, new JSONOptions(option, tz.getId))
    gen.write(row)
    gen.writeLineEnding
    gen.flush
    line.toString.getBytes(StandardCharsets.UTF_8)
  }
}
