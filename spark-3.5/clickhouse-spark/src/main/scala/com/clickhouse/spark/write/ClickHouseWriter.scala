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

package com.clickhouse.spark.write

import com.clickhouse.spark.{CommitMessage, Logging, TaskMetric, Utils}
import com.clickhouse.client.ClickHouseProtocol
import com.clickhouse.data.ClickHouseCompression
import com.clickhouse.spark.exception.{CHClientException, RetryableCHException}
import com.clickhouse.spark.spec.{DistributedEngineSpec, ShardUtils}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, TransformExpression}
import org.apache.spark.sql.catalyst.expressions.{Projection, SafeProjection}
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.clickhouse.ExprUtils
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import com.clickhouse.spark.Metrics._
import com.clickhouse.spark.io.{ForwardingOutputStream, ObservableOutputStream}
import com.clickhouse.spark._
import com.clickhouse.spark.client.{ClusterClient, NodeClient, NodeClientCache}
import com.clickhouse.spark.exception._
import com.clickhouse.spark.write.coalesce.{BatchHandle, BatchPayload, BucketContext, InsertCoordinator, ShardKey}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import java.util.UUID
import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

abstract class ClickHouseWriter(writeJob: WriteJobDescription)
    extends DataWriter[InternalRow] with Logging {

  val database: String = writeJob.targetDatabase(writeJob.writeOptions.convertDistributedToLocal)
  val table: String = writeJob.targetTable(writeJob.writeOptions.convertDistributedToLocal)
  val codec: ClickHouseCompression = writeJob.writeOptions.compressionCodec
  val protocol: ClickHouseProtocol = writeJob.node.protocol

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
    case None => None
    case Some(v2Expr) =>
      val catalystExpr = ExprUtils.toCatalyst(v2Expr, writeJob.dataSetSchema.fields, writeJob.functionRegistry)
      catalystExpr match {
        case BoundReference(_, dataType, _)
            if dataType.isInstanceOf[ByteType] // list all integral types here because we can not access `IntegralType`
              || dataType.isInstanceOf[ShortType]
              || dataType.isInstanceOf[IntegerType]
              || dataType.isInstanceOf[LongType] =>
          Some(catalystExpr)
        case BoundReference(_, dataType, _) =>
          throw CHClientException(s"Invalid data type of sharding field: $dataType")
        case TransformExpression(function, _, _) =>
          function.resultType match {
            case ByteType | ShortType | IntegerType | LongType => Some(catalystExpr)
            case _ => throw CHClientException(s"Invalid data type of sharding field: ${function.resultType}")
          }
        case unsupported: Expression =>
          log.warn(s"Unsupported expression of sharding field: $unsupported")
          None
      }
  }

  protected lazy val shardProjection: Option[expressions.Projection] = shardExpr
    .filter(_ => writeJob.writeOptions.convertDistributedToLocal)
    .flatMap {
      case expr: BoundReference =>
        Some(SafeProjection.create(Seq(expr)))
      case expr @ TransformExpression(function, _, _) =>
        // result type must be integer class
        function.resultType match {
          case ByteType => classOf[Byte]
          case ShortType => classOf[Short]
          case IntegerType => classOf[Int]
          case LongType => classOf[Long]
          case _ => throw CHClientException(s"Invalid return data type for function ${function.name()}," +
              s"sharding field: ${function.resultType}")
        }
        Some(SafeProjection.create(Seq(ExprUtils.resolveTransformCatalyst(expr, Some(writeJob.tz.getId)))))
    }

  // put the node select strategy in executor side because we need to calculate shard and don't know the records
  // util DataWriter#write(InternalRow) invoked.
  protected lazy val client: Either[ClusterClient, NodeClient] =
    writeJob.tableEngineSpec match {
      case _: DistributedEngineSpec
          if writeJob.writeOptions.useClusterNodesForDistributed || writeJob.writeOptions.convertDistributedToLocal =>
        val clusterSpec = writeJob.cluster.get
        log.info(s"Connect to cluster ${clusterSpec.name}, which has ${clusterSpec.shards.length} shards and " +
          s"${clusterSpec.nodes.length} nodes.")
        Left(ClusterClient(clusterSpec))
      case _ =>
        val nodeSpec = writeJob.node
        log.info(s"Connect to single node: $nodeSpec")
        Right(NodeClientCache.get(nodeSpec))
    }

  def nodeClient(shardNum: Option[Int]): NodeClient = client match {
    case Left(clusterClient) => clusterClient.node(shardNum)
    case Right(nodeClient) => nodeClient
  }

  def calcShard(record: InternalRow): Option[Int] = (shardExpr, shardProjection) match {
    case (Some(BoundReference(_, dataType, _)), Some(projection)) =>
      doCalcShard(record, dataType, projection)
    case (Some(TransformExpression(function, _, _)), Some(projection)) =>
      doCalcShard(record, function.resultType, projection)
    case _ => None
  }

  private def doCalcShard(record: InternalRow, dataType: DataType, projection: Projection): Option[Int] = {
    val shardValue = dataType match {
      case ByteType => Some(projection(record).getByte(0).toLong)
      case ShortType => Some(projection(record).getShort(0).toLong)
      case IntegerType => Some(projection(record).getInt(0).toLong)
      case LongType => Some(projection(record).getLong(0))
      case _ => None
    }
    shardValue.map(value => ShardUtils.calcShard(writeJob.cluster.get, value).num)
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
  val _totalWriteTime = new LongAdder
  def totalWriteTime: Long = _totalWriteTime.longValue

  // Lazily allocated: this 64 MB buffer and the output/compression chain below back the DIRECT
  // (non-coalesce) serialization path only. In coalesce mode they are never used, so they are
  // never instantiated (the ctor renew, reset(), and close() below are guarded by !coalesceActive).
  lazy val serializedBuffer = new ByteArrayOutputStream(64 * 1024 * 1024)

  // it is not accurate when using http protocol, because we delegate compression to
  // clickhouse http client
  private lazy val observableSerializedOutput = new ObservableOutputStream(
    serializedBuffer,
    Some(_lastSerializedBytesWritten),
    Some(_totalSerializedBytesWritten)
  )

  private val compressedForwardingOutput = new ForwardingOutputStream()

  private val observableCompressedOutput = new ObservableOutputStream(
    compressedForwardingOutput,
    Some(_currentRawBytesWritten),
    Some(_totalRawBytesWritten),
    Some(_lastSerializeTime),
    Some(_totalSerializeTime)
  )

  def output: OutputStream = observableCompressedOutput

  private def renewCompressedOutput(): Unit = {
    val compressedOutput = (codec, protocol) match {
      case (ClickHouseCompression.NONE, _) => observableSerializedOutput
      case (ClickHouseCompression.LZ4, ClickHouseProtocol.HTTP) =>
        // clickhouse http client forces compressed output stream
        // new Lz4OutputStream(observableSerializedOutput, 4 * 1024 * 1024, null)
        observableSerializedOutput
      case unsupported =>
        throw CHClientException(s"unsupported compression codec: $unsupported")
    }
    compressedForwardingOutput.updateDelegate(compressedOutput)
  }

  // Only the direct path uses the compression chain; coalesce serializes via serializeRecordBatch.
  // Guarding this (and reset()/close()) keeps the lazy 64 MB buffer from ever being allocated in
  // coalesce mode, and wires the forwarding delegate before the first write on the direct path.
  if (!coalesceActive) renewCompressedOutput()

  override def currentMetricsValues: Array[CustomTaskMetric] = Array(
    // Include work already handed off to the coordinator but not yet credited (coalesce mode). On
    // the direct path submittedRows/submittedBytes are always 0, so this reduces to the committed
    // totals. In coalesce mode the authoritative credit happens in commit() (see below), AFTER
    // Spark takes its final sample here, so without this term every pre-commit sample would read 0.
    // Adding the in-flight term here keeps the one source of truth in commit() (no double counting):
    // commit() resets these to 0 once it credits them, so a post-commit sample stays correct too.
    TaskMetric(RECORDS_WRITTEN, totalRecordsWritten + submittedRows),
    TaskMetric(BYTES_WRITTEN, totalSerializedBytesWritten + submittedBytes),
    TaskMetric(SERIALIZE_TIME, totalSerializeTime),
    TaskMetric(WRITE_TIME, totalWriteTime)
  )

  def format: String

  var currentShardNum: Option[Int] = None

  // Coalescing hooks. Default: not supported (JSON writer); the Arrow writer overrides these.
  def coalesceSupported: Boolean = false
  def serializeRecordBatch(): BatchPayload =
    throw new UnsupportedOperationException("record-batch serialization not supported for this format")
  def schemaHeaderBytes: Array[Byte] =
    throw new UnsupportedOperationException("schema header not supported for this format")

  private lazy val coalesceConfig = writeJob.writeOptions.coalesceConfig
  private def coalesceActive: Boolean = coalesceConfig.enabled && coalesceSupported

  private val pendingAcks = ArrayBuffer.empty[Future[Unit]]
  // Buckets this writer holds a reference on (acquired on first submit, released in close()).
  private val acquiredBuckets = scala.collection.mutable.Set.empty[ShardKey]
  // In-flight coalesce accounting: rows and serialized bytes handed to the coordinator but not yet
  // credited to the totals. Touched only on the task thread (doCoalescedFlush / commit /
  // currentMetricsValues), so plain vars are safe; the background sender threads never read them.
  private var submittedRows = 0L
  private var submittedBytes = 0L

  override def write(record: InternalRow): Unit = {
    val shardNum = calcShard(record)
    flush(force = shardNum != currentShardNum && currentBufferedRows > 0, currentShardNum)
    currentShardNum = shardNum
    val (_, serializedTime) = Utils.timeTakenMs(writeRow(record))
    _lastSerializeTime.add(serializedTime)
    _totalSerializeTime.add(serializedTime)
    _currentBufferedRows.add(1)
    flush(force = false, currentShardNum)
  }

  def writeRow(record: InternalRow): Unit

  def serialize(): Array[Byte] = {
    val (data, serializedTime) = Utils.timeTakenMs(doSerialize())
    _lastSerializeTime.add(serializedTime)
    _totalSerializeTime.add(serializedTime)
    data
  }

  def doSerialize(): Array[Byte]

  def reset(): Unit = {
    _currentBufferedRows.reset()
    _currentRawBytesWritten.reset()
    _lastSerializedBytesWritten.reset()
    _lastSerializeTime.reset()
    currentShardNum = None
    // The compression chain is unused in coalesce mode; don't force-allocate it on reset.
    if (!coalesceActive) {
      serializedBuffer.reset()
      renewCompressedOutput()
    }
  }

  def flush(force: Boolean, shardNum: Option[Int]): Unit =
    if (force) {
      doFlush(shardNum)
    } else if (currentBufferedRows >= writeJob.writeOptions.batchSize) {
      doFlush(shardNum)
    }

  def doFlush(shardNum: Option[Int]): Unit =
    if (coalesceActive) doCoalescedFlush(shardNum) else doDirectFlush(shardNum)

  private def doCoalescedFlush(shardNum: Option[Int]): Unit = {
    if (currentBufferedRows == 0) return
    val key = ShardKey(writeJob.queryId, database, table, shardNum)
    val payload = serializeRecordBatch()
    val handle = BatchHandle(key, UUID.randomUUID().toString, currentBufferedRows, payload)
    val ctx = BucketContext(
      client = nodeClient(shardNum),
      database = database,
      table = table,
      schemaHeader = schemaHeaderBytes,
      baseSettings = Map.empty,
      config = coalesceConfig
    )
    if (acquiredBuckets.add(key)) InsertCoordinator.acquire(key, ctx)
    pendingAcks += InsertCoordinator.submit(handle)
    submittedRows += currentBufferedRows
    // Count the serialized record-batch bytes (payload size) so BYTES_WRITTEN is non-zero in
    // coalesce mode. The direct path counts these via the observable output stream; the coalesce
    // Arrow path bypasses that stream, so take the size from the payload we just serialized.
    submittedBytes += payload.sizeBytes
    reset()
  }

  private def doDirectFlush(shardNum: Option[Int]): Unit = {
    val client = nodeClient(shardNum)
    val data = serialize()
    var writeTime = 0L
    Utils.retry[Unit, RetryableCHException](
      writeJob.writeOptions.maxRetry,
      writeJob.writeOptions.retryInterval
    ) {
      var startWriteTime = System.currentTimeMillis
      // codec,
      client.syncInsertOutputJSONEachRow(database, table, format, new ByteArrayInputStream(data)) match {
        case Right(_) =>
          writeTime = System.currentTimeMillis - startWriteTime
          _totalWriteTime.add(writeTime)
          _totalRecordsWritten.add(currentBufferedRows)
        case Left(retryable) if writeJob.writeOptions.retryableErrorCodes.contains(retryable.code) =>
          startWriteTime = System.currentTimeMillis
          throw RetryableCHException(retryable.code, retryable.reason, Some(client.nodeSpec))
        case Left(rethrow) => throw rethrow
      }
    } match {
      case Success(_) =>
        log.info(
          s"""Job[${writeJob.queryId}]: batch write completed
             |cluster: ${writeJob.cluster.map(_.name).getOrElse("none")}, shard: ${shardNum.getOrElse("none")}
             |node: ${client.nodeSpec}
             |        row count: $currentBufferedRows
             |         raw size: ${Utils.bytesToString(currentBufferedRawBytes)}
             |           format: $format
             |compression codec: $codec
             |  serialized size: ${Utils.bytesToString(lastSerializedBytesWritten)}
             |   serialize time: ${lastSerializeTime}ms
             |       write time: ${writeTime}ms
             |""".stripMargin
        )
        reset()
      case Failure(rethrow) => throw rethrow
    }
  }

  override def commit(): WriterCommitMessage =
    if (coalesceActive) {
      flush(force = currentBufferedRows > 0, currentShardNum)
      acquiredBuckets.foreach(InsertCoordinator.flush)
      // Block until every submitted group is acked; a failed insert rethrows here (before the
      // credit below), so we never permanently over-count on failure, matching the direct path.
      pendingAcks.foreach(f => Await.result(f, Duration.Inf))
      // Authoritative credit: move the in-flight tallies into the totals, then zero them so a
      // post-commit currentMetricsValues sample reports the same figure (no double counting).
      _totalRecordsWritten.add(submittedRows)
      _totalSerializedBytesWritten.add(submittedBytes)
      submittedRows = 0L
      submittedBytes = 0L
      CommitMessage(s"Job[${writeJob.queryId}]: commit (coalesced)")
    } else {
      flush(currentBufferedRows > 0, currentShardNum)
      CommitMessage(s"Job[${writeJob.queryId}]: commit")
    }

  override def abort(): Unit = {}

  override def close(): Unit = {
    // Spark calls close() after commit() or abort(), so this is where the writer's bucket
    // references are dropped; the last writer of a job's bucket removes it from the coordinator.
    acquiredBuckets.foreach(InsertCoordinator.release)
    acquiredBuckets.clear()
    // The shared NodeClientCache owns client lifetime (closed at executor shutdown). Only
    // per-task resources are released here; the Arrow root/allocator are closed by subclasses.
    // In coalesce mode the output/compression chain is never instantiated, so skip it (avoids
    // forcing the lazy 64 MB buffer and a null forwarding-delegate NPE at teardown).
    if (!coalesceActive) IOUtils.closeQuietly(output)
  }
}
