package xenon.clickhouse.read

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.{ClickHouseAnalysisException, ClickHouseSQLConf}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.SchemaUtil.fromClickHouseType
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.{ClickHouseClientException, ClickHouseServerException}
import xenon.clickhouse.spec.DistributedEngineSpec
import xenon.clickhouse.{ClickHouseHelper, GrpcClusterClient, GrpcNodeClient, Logging}
import xenon.protocol.grpc.Result
import xenon.clickhouse.exception.ClickHouseErrCode._

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.math.BigDecimal.RoundingMode

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

  lazy val iterator: Iterator[Array[JsonNode]] = grpcNodeClient.syncQueryWithStreamOutput(
    s"""
       | SELECT
       |  ${if (readSchema.isEmpty) 1 else readSchema.map(field => s"`${field.name}`").mkString(", ")}
       | FROM `$database`.`$table`
       | WHERE (${jobDesc.filterExpr})
       | -- AND ( shardExpr )
       | -- AND ( partExpr )
       |""".stripMargin
  )
    .map { result => onReceiveStreamResult(result); result }
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

  override def close(): Unit = grpcClient match {
    case Left(clusterClient) => clusterClient.close()
    case Right(nodeClient) => nodeClient.close()
  }

  def onReceiveStreamResult(result: Result): Unit = result match {
    case _ if result.getException.getCode == OK.code =>
    case _ => throw new ClickHouseServerException(result.getException)
  }
}

class ClickHouseColumnarReader {}
