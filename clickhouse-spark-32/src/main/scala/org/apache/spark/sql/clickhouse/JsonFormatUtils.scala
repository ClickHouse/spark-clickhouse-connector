package org.apache.spark.sql.clickhouse

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.StructType

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.time.ZoneId

object JsonFormatUtils {

  private val option = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss"
  )

  // inefficiently
  def row2Json(row: InternalRow, schema: StructType, tz: ZoneId): ByteString = {
    val line = ByteString.newOutput()

    val gen = new JacksonGenerator(
      schema,
      new OutputStreamWriter(line, StandardCharsets.UTF_8),
      new JSONOptions(option, tz.getId)
    )
    gen.write(row)
    gen.writeLineEnding
    gen.flush

    line.toByteString
  }
}
