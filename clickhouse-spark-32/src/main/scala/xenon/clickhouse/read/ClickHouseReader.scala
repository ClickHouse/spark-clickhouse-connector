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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.format.StreamOutput
import xenon.clickhouse.grpc.{GrpcNodeClient, GrpcNodesClient}
import xenon.clickhouse.{ClickHouseHelper, Logging}

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.math.BigDecimal.RoundingMode

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

  lazy val streamOutput: StreamOutput[Array[JsonNode]] = grpcNodeClient.syncStreamQuery(
    s"""
       | SELECT
       |  ${if (readSchema.isEmpty) 1 else readSchema.map(field => s"`${field.name}`").mkString(", ")}
       | FROM `$database`.`$table`
       | WHERE (${scanJob.filterExpr})
       | AND ( ${part.partFilterExpr} )
       |""".stripMargin
  )

  private var currentRow: Array[JsonNode] = _

  override def next(): Boolean = {
    val hasNext = streamOutput.hasNext
    if (hasNext) currentRow = streamOutput.next
    hasNext
  }

  override def get(): InternalRow =
    InternalRow.fromSeq((currentRow zip readSchema).map {
      case (null, StructField(_, _, true, _)) => null
      case (_: NullNode, StructField(_, _, true, _)) => null
      case (jsonNode: JsonNode, StructField(name, dataType, _, _)) => dataType match {
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
          case _ => throw ClickHouseClientException(s"Unsupported catalyst type $name[$dataType]")
        }
    })

  override def close(): Unit = grpcClient.close()
}

class ClickHouseColumnarReader {}
