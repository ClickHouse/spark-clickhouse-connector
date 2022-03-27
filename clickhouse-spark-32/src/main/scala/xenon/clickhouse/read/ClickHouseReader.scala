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

package xenon.clickhouse.read

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.{ClickHouseHelper, Logging}
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.format.StreamOutput
import xenon.clickhouse.grpc.{GrpcNodeClient, GrpcNodesClient}

class ClickHouseReader(
  scanJob: ScanJobDescription,
  part: ClickHouseInputPartition
) extends PartitionReader[InternalRow]
    with ClickHouseHelper
    with SQLConfHelper
    with Logging {

  val readDistributedUseClusterNodes: Boolean = conf.getConf(READ_DISTRIBUTED_USE_CLUSTER_NODES)
  val readDistributedConvertLocal: Boolean = conf.getConf(READ_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = part.table.database
  val table: String = part.table.name

  def readSchema: StructType = scanJob.readSchema

  private lazy val grpcClient: GrpcNodesClient = GrpcNodesClient(part.candidateNodes)

  def grpcNodeClient: GrpcNodeClient = grpcClient.node

  lazy val streamOutput: StreamOutput[Array[JsonNode]] = {
    val selectItems =
      if (readSchema.isEmpty) "1" // for case like COUNT(*) which prunes all columns
      else readSchema.map {
        field => if (scanJob.groupByClause.isDefined) field.name else s"`${field.name}`"
      }.mkString(", ")
    grpcNodeClient.syncStreamQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(
      s"""SELECT $selectItems
         |FROM `$database`.`$table`
         |WHERE (${part.partFilterExpr}) AND (${scanJob.filtersExpr})
         |${scanJob.groupByClause.getOrElse("")}
         |""".stripMargin
    )
  }

  private var currentRow: Array[JsonNode] = _

  override def next(): Boolean = {
    val hasNext = streamOutput.hasNext
    if (hasNext) currentRow = streamOutput.next
    hasNext
  }

  override def get(): InternalRow = InternalRow.fromSeq(
    (currentRow zip readSchema).map { case (jsonNode, structField) => decode(jsonNode, structField) }
  )

  private def decode(jsonNode: JsonNode, structField: StructField): Any = {
    if (jsonNode == null || jsonNode.isNull) {
      // should we check `structField.nullable`?
      return null
    }

    structField.dataType match {
      case BooleanType => jsonNode.asBoolean
      case ByteType => jsonNode.asInt.byteValue
      case ShortType => jsonNode.asInt.shortValue
      case IntegerType => jsonNode.asInt
      case LongType => jsonNode.asLong
      case FloatType => jsonNode.asDouble.floatValue
      case DoubleType => jsonNode.asDouble
      case d: DecimalType => jsonNode.decimalValue.setScale(d.scale, RoundingMode.HALF_UP)
      case TimestampType =>
        ZonedDateTime.parse(jsonNode.asText, dateTimeFmt.withZone(scanJob.tz))
          .withZoneSameInstant(ZoneOffset.UTC)
          .toEpochSecond * 1000 * 1000
      case StringType => UTF8String.fromString(jsonNode.asText)
      case DateType => LocalDate.parse(jsonNode.asText, dateFmt).toEpochDay
      case BinaryType => jsonNode.binaryValue
      case ArrayType(_dataType, _nullable) =>
        val _structField = StructField(s"${structField.name}__array_element__", _dataType, _nullable)
        new GenericArrayData(jsonNode.asScala.map(decode(_, _structField)))
      case MapType(StringType, _valueType, _valueNullable) =>
        val mapData = jsonNode.fields.asScala.map { entry =>
          val _structField = StructField(s"${structField.name}__map_value__", _valueType, _valueNullable)
          UTF8String.fromString(entry.getKey) -> decode(entry.getValue, _structField)
        }.toMap
        ArrayBasedMapData(mapData)
      case _ =>
        throw ClickHouseClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }

  override def close(): Unit = grpcClient.close()
}

class ClickHouseColumnarReader {}
