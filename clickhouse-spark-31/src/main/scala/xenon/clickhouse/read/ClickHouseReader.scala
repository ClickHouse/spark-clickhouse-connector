package xenon.clickhouse.read

import java.time.{LocalDate, ZonedDateTime, ZoneOffset}

import scala.math.BigDecimal.RoundingMode

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.{ClickHouseHelper, Logging}
import xenon.clickhouse.SchemaUtil.fromClickHouseType
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.{ClickHouseClientException, ClickHouseServerException}
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.grpc.{GrpcNodeClient, GrpcNodesClient}
import xenon.protocol.grpc.Result

class ClickHouseReader(
  jobDesc: ScanJobDesc,
  part: ClickHouseInputPartition
) extends PartitionReader[InternalRow]
    with ClickHouseHelper
    with SQLConfHelper
    with Logging {

  val readDistributedUseClusterNodes: Boolean = conf.getConf(ClickHouseSQLConf.READ_DISTRIBUTED_USE_CLUSTER_NODES)
  val readDistributedConvertLocal: Boolean = conf.getConf(ClickHouseSQLConf.READ_DISTRIBUTED_CONVERT_LOCAL)

  val database: String = part.table.database
  val table: String = part.table.name

  def readSchema: StructType = jobDesc.readSchema

  private lazy val grpcClient: GrpcNodesClient = GrpcNodesClient(part.candidateNodes)

  def grpcNodeClient: GrpcNodeClient = grpcClient.node

  lazy val iterator: Iterator[Array[JsonNode]] = grpcNodeClient.syncQueryWithStreamOutput(
    s"""
       | SELECT
       |  ${if (readSchema.isEmpty) 1 else readSchema.map(field => s"`${field.name}`").mkString(", ")}
       | FROM `$database`.`$table`
       | WHERE (${jobDesc.filterExpr})
       | AND ( ${part.partFilterExpr} )
       |""".stripMargin
  )
    .flatMap(_.getOutput.linesIterator)
    .filter(_.nonEmpty)
    .map(line => om.readValue[Array[JsonNode]](line))

  private var currentRow: Array[JsonNode] = _

  private var names: Array[String] = _
  private var types: Array[(DataType, Boolean)] = _

  override def next(): Boolean = {
    while (iterator.hasNext)
      if (names == null)
        names = iterator.next.map(_.asText)
      else if (types == null)
        types = iterator.next.map(_.asText).map(fromClickHouseType)
      else {
        currentRow = iterator.next
        return true
      }
    false
  }

  override def get(): InternalRow =
    InternalRow.fromSeq((currentRow zip readSchema).map {
      case (null, StructField(_, _, true, _)) => null
      case (_: NullNode, StructField(_, _, true, _)) => null
      case (jsonNode: JsonNode, StructField(name, dataType, _, _)) => dataType match {
          case BooleanType => jsonNode.asBoolean
          case ByteType => jsonNode.asInt.byteValue
          case ShortType => jsonNode.asInt.byteValue
          case IntegerType => jsonNode.asInt
          case LongType => jsonNode.asLong
          case FloatType => jsonNode.asDouble.floatValue
          case DoubleType => jsonNode.asDouble
          case d: DecimalType => jsonNode.decimalValue.setScale(d.scale, RoundingMode.HALF_UP)
          case TimestampType =>
            ZonedDateTime.parse(jsonNode.asText, dateTimeFmt.withZone(jobDesc.tz))
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
