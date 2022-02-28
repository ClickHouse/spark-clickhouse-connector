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

package xenon.clickhouse

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.READ_DISTRIBUTED_CONVERT_LOCAL
import org.apache.spark.sql.clickhouse.{ExprUtils, ReadOptions, WriteOptions}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.Utils._
import xenon.clickhouse.expr.{Expr, OrderExpr}
import xenon.clickhouse.grpc.GrpcNodeClient
import xenon.clickhouse.read.{ClickHouseMetadataColumn, ClickHouseScanBuilder, ScanJobDescription}
import xenon.clickhouse.spec._
import xenon.clickhouse.write.{ClickHouseWriteBuilder, WriteJobDescription}
import java.lang.{Integer => JInt, Long => JLong}
import java.time.ZoneId
import java.util

import scala.collection.JavaConverters._
import scala.util.Using

import org.apache.spark.unsafe.types.UTF8String

class ClickHouseTable(
  node: NodeSpec,
  cluster: Option[ClusterSpec],
  implicit val tz: ZoneId,
  spec: TableSpec,
  engineSpec: TableEngineSpec
) extends Table
    with SupportsRead
    with SupportsWrite
    with SupportsMetadataColumns
    with SupportsPartitionManagement
    with ClickHouseHelper
    with SQLConfHelper
    with SQLHelper
    with Logging {

  def database: String = spec.database

  def table: String = spec.name

  def isDistributed: Boolean = engineSpec.is_distributed

  val readDistributedConvertLocal: Boolean = conf.getConf(READ_DISTRIBUTED_CONVERT_LOCAL)

  lazy val (localTableSpec, localTableEngineSpec): (Option[TableSpec], Option[MergeTreeFamilyEngineSpec]) =
    engineSpec match {
      case distSpec: DistributedEngineSpec => Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
          val _localTableSpec = queryTableSpec(distSpec.local_db, distSpec.local_table)
          val _localTableEngineSpec =
            TableEngineUtils.resolveTableEngine(_localTableSpec).asInstanceOf[MergeTreeFamilyEngineSpec]
          (Some(_localTableSpec), Some(_localTableEngineSpec))
        }
      case _ => (None, None)
    }

  def shardingKey: Option[Expr] = engineSpec match {
    case _spec: DistributedEngineSpec => _spec.sharding_key
    case _ => None
  }

  def partitionKey: Option[List[Expr]] = engineSpec match {
    case mergeTreeFamilySpec: MergeTreeFamilyEngineSpec => Some(mergeTreeFamilySpec.partition_key.exprList)
    case _: DistributedEngineSpec => localTableEngineSpec.map(_.partition_key.exprList)
    case _: TableEngineSpec => None
  }

  def sortingKey: Option[List[OrderExpr]] = engineSpec match {
    case mergeTreeFamilySpec: MergeTreeFamilyEngineSpec => Some(mergeTreeFamilySpec.sorting_key).filter(_.nonEmpty)
    case _: DistributedEngineSpec => localTableEngineSpec.map(_.sorting_key).filter(_.nonEmpty)
    case _: TableEngineSpec => None
  }

  override def name: String = s"${wrapBackQuote(spec.database)}.${wrapBackQuote(spec.name)}"

  override def capabilities(): util.Set[TableCapability] =
    Set(
      BATCH_READ,
      BATCH_WRITE,
      TRUNCATE,
      ACCEPT_ANY_SCHEMA // TODO check schema and handle extra columns before writing
    ).asJava

  override lazy val schema: StructType = Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
    queryTableSchema(database, table)
  }

  /**
   * Only support `MergeTree` and `Distributed` table engine, for reference
   * {{{NamesAndTypesList MergeTreeData::getVirtuals()}}} {{{NamesAndTypesList StorageDistributed::getVirtuals()}}}
   */
  override lazy val metadataColumns: Array[MetadataColumn] = {

    def metadataCols(tableEngine: TableEngineSpec): Array[MetadataColumn] = tableEngine match {
      case _: MergeTreeFamilyEngineSpec => ClickHouseMetadataColumn.mergeTreeMetadataCols
      case _: DistributedEngineSpec => ClickHouseMetadataColumn.distributeMetadataCols
      case _ => Array.empty
    }

    engineSpec match {
      case _: DistributedEngineSpec if readDistributedConvertLocal => metadataCols(localTableEngineSpec.get)
      case other: TableEngineSpec => metadataCols(other)
    }
  }

  private lazy val metadataSchema: StructType =
    StructType(metadataColumns.map(_.asInstanceOf[ClickHouseMetadataColumn].toStructField))

  override lazy val partitioning: Array[Transform] = ExprUtils.toSparkParts(shardingKey, partitionKey)

  override lazy val partitionSchema: StructType = StructType(
    partitioning.map(partTransform => ExprUtils.inferTransformSchema(schema, metadataSchema, partTransform))
  )

  override lazy val properties: util.Map[String, String] = spec.toJavaMap

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val scanJob = ScanJobDescription(
      node = node,
      tz = tz,
      tableSpec = spec,
      tableEngineSpec = engineSpec,
      cluster = cluster,
      localTableSpec = localTableSpec,
      localTableEngineSpec = localTableEngineSpec,
      readOptions = new ReadOptions(options.asCaseSensitiveMap())
    )
    // TODO schema of partitions
    val partTransforms = Array[Transform]()
    new ClickHouseScanBuilder(scanJob, schema, metadataSchema, partTransforms)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): ClickHouseWriteBuilder = {
    val writeJob = WriteJobDescription(
      queryId = info.queryId,
      dataSetSchema = info.schema,
      node = node,
      tz = tz,
      tableSpec = spec,
      tableEngineSpec = engineSpec,
      cluster = cluster,
      localTableSpec = localTableSpec,
      localTableEngineSpec = localTableEngineSpec,
      shardingKey = shardingKey,
      partitionKey = partitionKey,
      sortingKey = sortingKey,
      writeOptions = new WriteOptions(info.options.asCaseSensitiveMap())
    )

    new ClickHouseWriteBuilder(writeJob)
  }

  override def createPartition(ident: InternalRow, props: util.Map[String, String]): Unit =
    log.info("Do nothing on ClickHouse for creating partition action")

  override def dropPartition(ident: InternalRow): Boolean = {
    if (isDistributed)
      throw new UnsupportedOperationException(s"Unsupported operation: drop partition on Distributed table")

    val partitionExpr = (0 until ident.numFields).map { i =>
      partitionSchema.fields(i).dataType match {
        case IntegerType => compileValue(ident.getInt(i))
        case LongType => compileValue(ident.getLong(i))
        case StringType => compileValue(ident.getUTF8String(i))
        case illegal => throw new IllegalArgumentException(s"Illegal partition data type: $illegal")
      }
    }.mkString("(", ",", ")")

    Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
      dropPartition(database, table, partitionExpr)
    }
  }

  override def replacePartitionMetadata(ident: InternalRow, props: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException("Unsupported operation: replacePartitionMetadata")

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] =
    throw new UnsupportedOperationException("Unsupported operation: loadPartitionMetadata")

  override def listPartitionIdentifiers(names: Array[String], ident: InternalRow): Array[InternalRow] = {
    assert(
      names.length == ident.numFields,
      s"Number of partition names (${names.length}) must be equal to " +
        s"the number of partition values (${ident.numFields})."
    )
    assert(
      names.forall(fieldName => partitionSchema.fieldNames.contains(fieldName)),
      s"Some partition names ${names.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${partitionSchema.sql}'."
    )

    def strToSparkValue(str: String, dataType: DataType): Any = dataType match {
      case StringType => UTF8String.fromString(str.stripPrefix("'").stripSuffix("'"))
      case IntegerType => JInt.parseInt(str)
      case LongType => JLong.parseLong(str)
      case unsupported => throw new UnsupportedOperationException(s"$unsupported")
    }

    Using.resource(GrpcNodeClient(node)) { implicit grpcNodeClient =>
      queryPartitionSpec(database, table).map(_.partition)
    }
      .filterNot(_ == "tuple()") // represent the root partition of un-partitioned table
      .map {
        case tuple if tuple.startsWith("(") && tuple.endsWith(")") =>
          tuple.stripPrefix("(").stripSuffix(")").split(",")
        case partColStrValue =>
          Array(partColStrValue)
      }
      .map { partColStrValues =>
        new GenericInternalRow(
          (partColStrValues zip partitionSchema.fields.map(_.dataType))
            .map { case (partColStrValue, dataType) => strToSparkValue(partColStrValue, dataType) }
        )
      }
      .filter { partRow =>
        names.zipWithIndex.forall { case (name, queryIndex) =>
          val partRowIndex = partitionSchema.fieldIndex(name)
          val dataType = partitionSchema.fields(partRowIndex).dataType
          partRow.get(partRowIndex, dataType) == ident.get(queryIndex, dataType)
        }
      }
      .toArray
  }
}
