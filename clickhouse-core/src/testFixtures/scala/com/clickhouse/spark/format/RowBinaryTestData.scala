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

package com.clickhouse.spark.format

import com.clickhouse.client.api.ClientConfigProperties
import com.clickhouse.client.api.data_formats.internal.SerializerUtils
import com.clickhouse.client.api.http.ClickHouseHttpProto
import com.clickhouse.client.api.internal.{ClickHouseLZ4InputStream, ClickHouseLZ4OutputStream}
import com.clickhouse.client.api.query.{QueryResponse, QuerySettings}
import com.clickhouse.data.{ClickHouseColumn, ClickHouseDataType, ClickHouseFormat}
import com.clickhouse.shaded.net.jpountz.lz4.lz4.LZ4Factory
import com.clickhouse.shaded.org.apache.hc.core5.http.ContentType
import com.clickhouse.shaded.org.apache.hc.core5.http.io.entity.InputStreamEntity
import com.clickhouse.shaded.org.apache.hc.core5.http.message.BasicClassicHttpResponse

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FilterInputStream, InputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.JavaConverters._

/** RowBinaryWithNamesAndTypes bytes written with client-v2's serializer, served as a client-v2 response. */
object RowBinaryTestData {

  /** Bytes written as they are in place of a value. */
  final case class Raw(bytes: Array[Byte])

  /** Map entries in wire order; duplicate keys allowed. */
  final case class Entries(pairs: (Any, Any)*)

  def int32LE(v: Int): Raw = Raw(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array())

  def int64LE(v: Long): Raw = Raw(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array())

  /** A String value with these exact bytes, valid UTF-8 or not. */
  def stringBytes(bytes: Array[Byte]): Raw = {
    val out = new ByteArrayOutputStream()
    SerializerUtils.writeVarInt(out, bytes.length)
    out.write(bytes)
    Raw(out.toByteArray)
  }

  def header(columns: Seq[(String, String)]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    SerializerUtils.writeVarInt(out, columns.size)
    columns.foreach { case (name, _) => writeString(out, name) }
    columns.foreach { case (_, chType) => writeString(out, chType) }
    out.toByteArray
  }

  /** Header and rows; values are what client-v2's `SerializerUtils` takes, Seq for arrays and tuples. */
  def encode(columns: Seq[(String, String)], rows: Seq[Seq[Any]]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    out.write(header(columns))
    val parsed = columns.map { case (name, chType) => ClickHouseColumn.of(name, chType) }
    rows.foreach { row =>
      require(row.size == parsed.size, s"row $row does not match ${columns.size} columns")
      row.zip(parsed).foreach { case (value, column) => write(out, value, column) }
    }
    out.toByteArray
  }

  def write(out: OutputStream, value: Any, column: ClickHouseColumn): Unit = value match {
    case Raw(bytes) => out.write(bytes)
    case null if column.isNullable => out.write(1)
    case _ =>
      if (column.isNullable) out.write(0)
      writeValue(out, value, column)
  }

  private def writeValue(out: OutputStream, value: Any, column: ClickHouseColumn): Unit =
    (column.getDataType, value) match {
      case (_, Raw(bytes)) => out.write(bytes)
      case (ClickHouseDataType.Array, items: Seq[_]) =>
        SerializerUtils.writeVarInt(out, items.size)
        items.foreach(write(out, _, column.getNestedColumns.get(0)))
      case (ClickHouseDataType.Map, entries: Entries) =>
        SerializerUtils.writeVarInt(out, entries.pairs.size)
        entries.pairs.foreach { case (k, v) =>
          write(out, k, column.getKeyInfo)
          write(out, v, column.getValueInfo)
        }
      case (ClickHouseDataType.Tuple, fields: Seq[_]) =>
        fields.zip(column.getNestedColumns.asScala).foreach { case (v, c) => write(out, v, c) }
      case _ => SerializerUtils.serializeData(out, value.asInstanceOf[AnyRef], column)
    }

  private def writeString(out: OutputStream, s: String): Unit = {
    val bytes = s.getBytes(StandardCharsets.UTF_8)
    SerializerUtils.writeVarInt(out, bytes.length)
    out.write(bytes)
  }

  /** client-v2's LZ4 block framing, as ClickHouse compresses responses. */
  def lz4(bytes: Array[Byte], blockSize: Int = 1024): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val lz4Out = new ClickHouseLZ4OutputStream(out, LZ4Factory.fastestJavaInstance().fastCompressor(), blockSize)
    lz4Out.write(bytes)
    lz4Out.close()
    out.toByteArray
  }

  def unlz4(in: InputStream): InputStream =
    new ClickHouseLZ4InputStream(in, LZ4Factory.fastestJavaInstance().fastDecompressor(), 64 * 1024)

  /** Settings as client-v2 passes them by default: dates in the server time zone. */
  val clientDefaults: Map[String, AnyRef] =
    Map(ClientConfigProperties.USE_SERVER_TIMEZONE.getKey -> java.lang.Boolean.TRUE)

  /** Settings with the `use_time_zone` client option. */
  def useTimeZone(timeZone: String): Map[String, AnyRef] = Map(
    ClientConfigProperties.USE_SERVER_TIMEZONE.getKey -> java.lang.Boolean.FALSE,
    ClientConfigProperties.USE_TIMEZONE.getKey -> java.util.TimeZone.getTimeZone(timeZone)
  )

  /** A response serving `stream`, with the time zone header ClickHouse sends. */
  def response(
    stream: InputStream,
    serverTimeZone: String = "UTC",
    settings: Map[String, AnyRef] = clientDefaults
  ): QueryResponse = {
    val http = new BasicClassicHttpResponse(200)
    http.addHeader(ClickHouseHttpProto.HEADER_TIMEZONE, serverTimeZone)
    http.setEntity(new InputStreamEntity(stream, ContentType.APPLICATION_OCTET_STREAM))
    val querySettings = new QuerySettings()
    settings.foreach { case (k, v) => querySettings.setOption(k, v) }
    new QueryResponse(http, ClickHouseFormat.RowBinaryWithNamesAndTypes, querySettings, null)
  }

  /** Returns at most `n` bytes per read, as a slow network does. */
  final class Trickle(in: InputStream, n: Int = 1) extends FilterInputStream(in) {
    override def read(b: Array[Byte], off: Int, len: Int): Int = super.read(b, off, math.min(len, n))
  }

  /** Serves `bytes`, then throws `failure` instead of ending. */
  final class FailingStream(bytes: Array[Byte], failure: Exception) extends InputStream {
    private val in = new ByteArrayInputStream(bytes)
    override def read(): Int = {
      val b = in.read()
      if (b < 0) throw failure else b
    }
    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      val n = in.read(b, off, len)
      if (n < 0 && len > 0) throw failure else n
    }
  }
}
