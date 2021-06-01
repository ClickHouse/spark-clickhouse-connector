package xenon.clickhouse.read

import java.time.{LocalDate, ZonedDateTime, ZoneOffset}

import scala.math.BigDecimal.RoundingMode

import com.fasterxml.jackson.databind.node.NullNode
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.{ClickHouseAnalysisException, ClickHouseSQLConf}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.{ClickHouseHelper, GrpcClusterClient, GrpcNodeClient, Logging}
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.format.JSONOutput
import xenon.clickhouse.spec.DistributedEngineSpec
import xenon.protocol.grpc.Result

class ClickHouseReader(jobDesc: ScanJobDesc)
    extends PartitionReader[InternalRow]
    with ClickHouseHelper
    with SQLConfHelper
    with Logging {

  val readDistributedUseClusterNodes: Boolean = conf.getConf(ClickHouseSQLConf.READ_DISTRIBUTED_USE_CLUSTER_NODES)
  val readDistributedConvertLocal: Boolean = conf.getConf(ClickHouseSQLConf.READ_DISTRIBUTED_CONVERT_LOCAL)

  def readSchema: StructType = jobDesc.readSchema

  val database: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if readDistributedConvertLocal => dist.local_db
    case _ => jobDesc.tableSpec.database
  }

  val table: String = jobDesc.tableEngineSpec match {
    case dist: DistributedEngineSpec if readDistributedConvertLocal => dist.local_table
    case _ => jobDesc.tableSpec.name
  }

  private lazy val grpcClient: Either[GrpcClusterClient, GrpcNodeClient] =
    (jobDesc.tableEngineSpec, readDistributedUseClusterNodes, readDistributedConvertLocal) match {
      case (_: DistributedEngineSpec, _, true) =>
        throw ClickHouseAnalysisException(
          s"${ClickHouseSQLConf.READ_DISTRIBUTED_CONVERT_LOCAL.key} is not support yet."
        )
      case (_: DistributedEngineSpec, true, _) =>
        val clusterSpec = jobDesc.cluster.get
        log.info(s"Connect to ClickHouse cluster ${clusterSpec.name}, which has ${clusterSpec.nodes.size} nodes.")
        Left(GrpcClusterClient(clusterSpec))
      case _ =>
        val nodeSpec = jobDesc.node
        log.info("Connect to ClickHouse single node.")
        Right(GrpcNodeClient(nodeSpec))
    }

  def grpcNodeClient: GrpcNodeClient = grpcClient match {
    case Left(clusterClient) => clusterClient.node()
    case Right(nodeClient) => nodeClient
  }

  lazy val iterator: Iterator[Result] = grpcNodeClient.syncQueryWithStreamOutput(
    s"""
       | SELECT 
       |  ${readSchema.map(field => s"`${field.name}`").mkString(", ")}
       | FROM `$database`.`$table`
       | WHERE (${jobDesc.filterExpr})
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
            ZonedDateTime.parse(jsonNode.asText, dateTimeFmt.withZone(jobDesc.tz))
              .withZoneSameInstant(ZoneOffset.UTC)
              .toEpochSecond * 1000 * 1000
          case StringType => UTF8String.fromString(jsonNode.asText)
          case DateType => LocalDate.parse(jsonNode.asText, dateFmt).toEpochDay
          case BinaryType => jsonNode.binaryValue
          case _ => throw ClickHouseClientException(s"Unsupported catalyst type ${field.name}[${field.dataType}]")
        }
      }
    })
  }

  override def close(): Unit = grpcClient match {
    case Left(clusterClient) => clusterClient.close()
    case Right(nodeClient) => nodeClient.close()
  }
}

class ClickHouseColumnarReader {}
