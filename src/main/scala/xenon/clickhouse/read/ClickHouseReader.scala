package xenon.clickhouse.read

import java.time.{LocalDate, ZonedDateTime, ZoneId, ZoneOffset}

import scala.math.BigDecimal.RoundingMode

import com.fasterxml.jackson.databind.node.NullNode
import com.zy.dp.xenon.protocol.grpc.Result
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.{ClickHouseHelper, GrpcNodeClient}
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.format.JSONOutput
import xenon.clickhouse.spec.{ClusterSpec, NodeSpec}

class ClickHouseReader(
  node: NodeSpec,
  cluster: Option[ClusterSpec] = None,
  tz: Either[ZoneId, ZoneId],
  database: String,
  table: String,
  readSchema: StructType,
  filterExpr: String
) extends PartitionReader[InternalRow]
    with ClickHouseHelper {

  private lazy val grpcConn: GrpcNodeClient = GrpcNodeClient(node)

  lazy val iterator: Iterator[Result] = grpcConn.syncQueryWithStreamOutput(
    s"""
       | SELECT 
       |  ${readSchema.map(field => s"`${field.name}`").mkString(", ")}
       | FROM `$database`.`$table`
       | WHERE ($filterExpr)
       | -- AND ( shardExpr )
       | -- AND ( partExpr )
       |""".stripMargin
  )

  var currentOutput: JSONOutput = _
  var currentOffset: Int = 0

  override def next(): Boolean = {
    if (currentOutput == null || currentOffset >= currentOutput.rows) {
      if (!iterator.hasNext) return false
      currentOutput = om.readValue[JSONOutput](iterator.next.getOutput)
      currentOffset = 0
    }
    true
  }

  override def get(): InternalRow = {
    val row = currentOutput.data(currentOffset)
    currentOffset = currentOffset + 1
    InternalRow.fromSeq(readSchema.map { field =>
      val jsonNode = row.get(field.name)
      if (field.nullable && (jsonNode == null || jsonNode.isInstanceOf[NullNode])) {
        null
      } else {
        field.dataType match {
          case BooleanType => jsonNode.asBoolean
          case ByteType => jsonNode.asInt.byteValue
          case ShortType => jsonNode.asInt.byteValue
          case IntegerType => jsonNode.asInt
          case LongType => jsonNode.asLong
          case FloatType => jsonNode.asDouble.floatValue
          case DoubleType => jsonNode.asDouble
          case d: DecimalType => jsonNode.decimalValue.setScale(d.scale, RoundingMode.HALF_UP)
          case TimestampType =>
            ZonedDateTime.parse(jsonNode.asText, dateTimeFmt.withZone(tz.merge))
              .withZoneSameInstant(ZoneOffset.UTC)
              .toEpochSecond * 1000 * 1000
          case StringType => UTF8String.fromString(jsonNode.asText)
          case DateType => LocalDate.parse(jsonNode.asText, dateFmt).toEpochDay
          case BinaryType => jsonNode.binaryValue
          case _ => throw ClickHouseClientException(s"unsupported catalyst type ${field.name}[${field.dataType}]")
        }
      }
    })
  }

  override def close(): Unit = grpcConn.close()
}

class ClickHouseColumnarReader {}
