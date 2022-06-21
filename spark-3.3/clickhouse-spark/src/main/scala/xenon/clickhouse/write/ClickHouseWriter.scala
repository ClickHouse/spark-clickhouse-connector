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

package xenon.clickhouse.write

import com.github.luben.zstd.{RecyclingBufferPool, ZstdOutputStreamNoFinalizer}
import com.google.protobuf.ByteString
import net.jpountz.lz4.LZ4FrameOutputStream
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, SafeProjection}
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.clickhouse.ExprUtils
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import xenon.clickhouse._
import xenon.clickhouse.exception._
import xenon.clickhouse.grpc.{GrpcClusterClient, GrpcNodeClient}
import xenon.clickhouse.io.{ForwardingOutputStream, ObservableOutputStream}
import xenon.clickhouse.spec.{DistributedEngineSpec, ShardUtils}

import java.io.OutputStream
import java.util.concurrent.atomic.LongAdder
import java.util.zip.GZIPOutputStream
import scala.util.{Failure, Success}

abstract class ClickHouseWriter(writeJob: WriteJobDescription)
    extends DataWriter[InternalRow] with Logging {

  val database: String = writeJob.targetDatabase(writeJob.writeOptions.convertDistributedToLocal)
  val table: String = writeJob.targetTable(writeJob.writeOptions.convertDistributedToLocal)
  val codec: Option[String] = writeJob.writeOptions.compressionCodec

  // ClickHouse is nullable sensitive, if the table column is not nullable, we need to cast the column
  // to be non-nullable forcibly.
  protected val revisedDataSchema: StructType = StructType(
    writeJob.dataSetSchema.map { field =>
      writeJob.tableSchema.find(_.name == field.name) match {
        case Some(tableField) if !tableField.nullable && field.nullable => field.copy(nullable = false)
        case _ => field
      }
    }
  )

  protected lazy val shardExpr: Option[Expression] = writeJob.sparkShardExpr match {
    case None if writeJob.tableEngineSpec.is_distributed =>
      throw ClickHouseClientException("Can not write data to a Distributed table that lacks sharding key")
    case None => None
    case Some(v2Expr) =>
      val catalystExpr = ExprUtils.toCatalyst(v2Expr, writeJob.dataSetSchema.fields)
      catalystExpr match {
        case BoundReference(_, dataType, _)
            if dataType.isInstanceOf[ByteType] // list all integral types here because we can not access `IntegralType`
              || dataType.isInstanceOf[ShortType]
              || dataType.isInstanceOf[IntegerType]
              || dataType.isInstanceOf[LongType] =>
          Some(catalystExpr)
        case BoundReference(_, dataType, _) =>
          throw ClickHouseClientException(s"Invalid data type of sharding field: $dataType")
        case unsupported: Expression =>
          log.warn(s"Unsupported expression of sharding field: $unsupported")
          None
      }
  }

  protected lazy val shardProjection: Option[expressions.Projection] = shardExpr
    .filter(_ => writeJob.writeOptions.convertDistributedToLocal)
    .map(expr => SafeProjection.create(Seq(expr)))

  // put the node select strategy in executor side because we need to calculate shard and don't know the records
  // util DataWriter#write(InternalRow) invoked.
  protected lazy val grpcClient: Either[GrpcClusterClient, GrpcNodeClient] =
    writeJob.tableEngineSpec match {
      case _: DistributedEngineSpec
          if writeJob.writeOptions.useClusterNodesForDistributed || writeJob.writeOptions.convertDistributedToLocal =>
        val clusterSpec = writeJob.cluster.get
        log.info(s"Connect to cluster ${clusterSpec.name}, which has ${clusterSpec.shards.length} shards and " +
          s"${clusterSpec.nodes.length} nodes.")
        Left(GrpcClusterClient(clusterSpec))
      case _ =>
        val nodeSpec = writeJob.node
        log.info(s"Connect to single node: $nodeSpec")
        Right(GrpcNodeClient(nodeSpec))
    }

  def grpcNodeClient(shardNum: Option[Int]): GrpcNodeClient = grpcClient match {
    case Left(clusterClient) => clusterClient.node(shardNum)
    case Right(nodeClient) => nodeClient
  }

  def calcShard(record: InternalRow): Option[Int] = (shardExpr, shardProjection) match {
    case (Some(BoundReference(_, dataType, _)), Some(projection)) =>
      val shardValue = dataType match {
        case ByteType => Some(projection(record).getByte(0).toLong)
        case ShortType => Some(projection(record).getShort(0).toLong)
        case IntegerType => Some(projection(record).getInt(0).toLong)
        case LongType => Some(projection(record).getLong(0))
        case _ => None
      }
      shardValue.map(value => ShardUtils.calcShard(writeJob.cluster.get, value).num)
    case _ => None
  }

  val _currentBufferedRows = new LongAdder
  def currentBufferedRows: Long = _currentBufferedRows.longValue
  val _totalRecordsWritten = new LongAdder
  def totalRecordsWritten: Long = _totalRecordsWritten.longValue
  val _currentRawBytesWritten = new LongAdder
  def currentBufferedRawBytes: Long = _currentRawBytesWritten.longValue
  val _totalRawBytesWritten = new LongAdder
  def totalRawBytesWritten: Long = _totalRawBytesWritten.longValue
  val _lastSerializedBytesWritten = new LongAdder
  def lastSerializedBytesWritten: Long = _lastSerializedBytesWritten.longValue
  val _totalSerializedBytesWritten = new LongAdder
  def totalSerializedBytesWritten: Long = _totalSerializedBytesWritten.longValue
  val _lastSerializeTime = new LongAdder
  def lastSerializeTime: Long = _lastSerializeTime.longValue
  val _totalSerializeTime = new LongAdder
  def totalSerializeTime: Long = _totalSerializeTime.longValue
  val _totalWrittenTime = new LongAdder
  def totalWrittenTime: Long = _totalWrittenTime.longValue

  // zstd only
  private lazy val bufferedForwardingOutput = new ForwardingOutputStream()

  val serializedBuffer: ByteString.Output = ByteString.newOutput(16 * 1024 * 1024)

  private val observableSerializedOutput = new ObservableOutputStream(
    serializedBuffer,
    Some(_lastSerializedBytesWritten),
    Some(_totalSerializedBytesWritten)
  )

  private val compressedForwardingOutput: ForwardingOutputStream = new ForwardingOutputStream()

  private val observableCompressedOutput = new ObservableOutputStream(
    compressedForwardingOutput,
    Some(_currentRawBytesWritten),
    Some(_totalRawBytesWritten),
    Some(_lastSerializeTime),
    Some(_totalSerializeTime)
  )

  def output: OutputStream = observableCompressedOutput

  private def renewCompressedOutput(): Unit = {
    val compressedOutput = codec.map(_.toLowerCase) match {
      case None => observableSerializedOutput
      case Some("gzip") =>
        new GZIPOutputStream(observableSerializedOutput, 4 * 1024 * 1024)
      case Some("lz4") =>
        new LZ4FrameOutputStream(observableSerializedOutput, BLOCKSIZE.SIZE_4MB)
      case Some("zstd") =>
        val zstdOutput = new ZstdOutputStreamNoFinalizer(observableSerializedOutput, RecyclingBufferPool.INSTANCE)
          .setLevel(writeJob.writeOptions.zstdLevel)
          .setWorkers(writeJob.writeOptions.zstdThread)
        bufferedForwardingOutput.updateDelegate(zstdOutput)
        bufferedForwardingOutput
      case unsupported =>
        throw ClickHouseClientException(s"unsupported compression codec: $unsupported")
    }
    compressedForwardingOutput.updateDelegate(compressedOutput)
  }
  renewCompressedOutput()

  override def currentMetricsValues: Array[CustomTaskMetric] = Array(
    WriteTaskMetric("recordsWritten", totalRecordsWritten),
    WriteTaskMetric("bytesWritten", totalSerializedBytesWritten),
    WriteTaskMetric("rawBytesWritten", totalRawBytesWritten),
    WriteTaskMetric("serializeTime", totalSerializeTime),
    WriteTaskMetric("writtenTime", totalWrittenTime)
  )

  def format: String

  var currentShardNum: Option[Int] = None

  override def write(record: InternalRow): Unit = {
    val shardNum = calcShard(record)
    flush(force = shardNum != currentShardNum && currentBufferedRows > 0, currentShardNum)
    currentShardNum = shardNum
    writeRow(record)
    _currentBufferedRows.add(1)
    flush(force = false, currentShardNum)
  }

  def writeRow(record: InternalRow): Unit

  def serialize(): ByteString = {
    val (data, serializedTime) = Utils.timeTakenMs(doSerialize())
    _lastSerializeTime.add(serializedTime)
    _totalSerializeTime.add(serializedTime)
    data
  }

  def doSerialize(): ByteString

  def reset(): Unit = {
    _currentBufferedRows.reset()
    _currentRawBytesWritten.reset()
    _lastSerializedBytesWritten.reset()
    _lastSerializeTime.reset()
    currentShardNum = None
    serializedBuffer.reset()
    renewCompressedOutput()
  }

  def flush(force: Boolean, shardNum: Option[Int]): Unit =
    if (force) {
      doFlush(shardNum)
    } else if (currentBufferedRows >= writeJob.writeOptions.batchSize) {
      doFlush(shardNum)
    }

  def doFlush(shardNum: Option[Int]): Unit = {
    val client = grpcNodeClient(shardNum)
    val data = serialize()
    var writeTime = 0L
    Utils.retry[Unit, RetryableClickHouseException](
      writeJob.writeOptions.maxRetry,
      writeJob.writeOptions.retryInterval
    ) {
      var startWriteTime = System.currentTimeMillis
      client.syncInsertOutputJSONEachRow(database, table, format, codec, data) match {
        case Right(_) =>
          writeTime = System.currentTimeMillis - startWriteTime
          _totalWrittenTime.add(writeTime)
          _totalRecordsWritten.add(currentBufferedRows)
        case Left(retryable) if writeJob.writeOptions.retryableErrorCodes.contains(retryable.getCode) =>
          startWriteTime = System.currentTimeMillis
          throw new RetryableClickHouseException(retryable, Some(client.node))
        case Left(rethrow) => throw new ClickHouseServerException(rethrow, Some(client.node))
      }
    } match {
      case Success(_) =>
        log.info(
          s"""Job[${writeJob.queryId}]: batch write completed
             |cluster: ${writeJob.cluster.map(_.name).getOrElse("none")}, shard: ${shardNum.getOrElse("none")}
             |node: ${client.node}
             |        row count: $currentBufferedRows
             |         raw size: ${Utils.bytesToString(currentBufferedRawBytes)}
             |           format: $format
             |compression codec: ${codec.getOrElse("none")}
             |  serialized size: ${Utils.bytesToString(lastSerializedBytesWritten)}
             |   serialize time: ${lastSerializeTime}ms
             |       write time: ${writeTime}ms
             |""".stripMargin
        )
        reset()
      case Failure(rethrow) => throw rethrow
    }
  }

  override def commit(): WriterCommitMessage = {
    flush(currentBufferedRows > 0, currentShardNum)
    CommitMessage(s"Job[${writeJob.queryId}]: commit")
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    IOUtils.closeQuietly(output)
    grpcClient match {
      case Left(clusterClient) => clusterClient.close()
      case Right(nodeClient) => nodeClient.close()
    }
  }
}
