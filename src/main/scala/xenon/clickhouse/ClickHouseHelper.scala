package xenon.clickhouse

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.Constants._
import xenon.clickhouse.Utils.{dateTimeFmt, om}
import xenon.clickhouse.format.JSONOutput
import xenon.clickhouse.spec.{ClusterSpec, DatabaseSpec, NodeSpec, ReplicaSpec, ShardSpec, TableSpec}

import java.time.{LocalDateTime, ZoneId}
import scala.collection.JavaConverters._

trait ClickHouseHelper {

  @volatile lazy val DEFAULT_ACTION_IF_NO_SUCH_DATABASE: String => Unit =
    (db: String) => throw new NoSuchDatabaseException(db)

  @volatile lazy val DEFAULT_ACTION_IF_NO_SUCH_TABLE: (String, String) => Unit =
    (database, table) => throw new NoSuchTableException(s"$database.$table")

  def quoted(token: String) = s"`$token`"

  def unwrap(ident: Identifier): Option[(String, String)] = ident.namespace() match {
    case Array(database) => Some((database, ident.name()))
    case _ => None
  }

  def buildNodeSpec(options: CaseInsensitiveStringMap): NodeSpec = {
    val host = options.getOrDefault(CATALOG_PROP_HOST, "localhost")
    val port = options.getInt(CATALOG_PROP_PORT, 9100)
    val user = options.getOrDefault(CATALOG_PROP_USER, "default")
    val password = options.getOrDefault(CATALOG_PROP_PASSWORD, "")
    val database = options.getOrDefault(CATALOG_PROP_DATABASE, "default")
    NodeSpec(_host = host, _grpc_port = Some(port), username = user, password = password, database = database)
  }

  def queryClusterSpecs(nodeSpec: NodeSpec)(implicit grpcNodeClient: GrpcNodeClient): Seq[ClusterSpec] = {
    val clustersResult = grpcNodeClient.syncQueryAndCheck(
      """ SELECT
        |   `cluster`,                 -- String
        |   `shard_num`,               -- UInt32
        |   `shard_weight`,            -- UInt32
        |   `replica_num`,             -- UInt32
        |   `host_name`,               -- String
        |   `host_address`,            -- String
        |   `port`,                    -- UInt16
        |   `is_local`,                -- UInt8
        |   `user`,                    -- String
        |   `default_database`,        -- String
        |   `errors_count`,            -- UInt32
        |   `estimated_recovery_time`  -- UInt32
        | FROM `system`.`clusters`
        |""".stripMargin
    )
    om.readValue[JSONOutput](clustersResult.getOutput).data
      .groupBy(_.get("cluster").asText)
      .map { case (cluster, rows) =>
        val shards = rows
          .groupBy(_.get("shard_num").asInt)
          .map { case (shardNum, rows) =>
            val shardWeight = rows.head.get("shard_weight").asInt
            val nodes = rows.map { row =>
              val replicaNum = row.get("replica_num").asInt
              // should other properties be provided by `SparkConf`?
              val clickhouseNode = nodeSpec.copy(
                // host_address is not works for testcontainers
                _host = row.get("host_name").asText,
                _tcp_port = Some(row.get("port").asInt)
              )
              ReplicaSpec(replicaNum, clickhouseNode)
            }.toArray
            ShardSpec(shardNum, shardWeight, nodes)
          }.toArray
        ClusterSpec(cluster, shards)
      }.toSeq
  }

  def queryDatabaseSpec(
    database: String,
    actionIfNoSuchDatabase: String => Unit = DEFAULT_ACTION_IF_NO_SUCH_DATABASE
  )(implicit grpcNodeClient: GrpcNodeClient): DatabaseSpec = {
    val result = grpcNodeClient.syncQueryAndCheck(
      s"""
         | SELECT
         |   `name`,          -- String
         |   `engine`,        -- String
         |   `data_path`,     -- String
         |   `metadata_path`, -- String
         |   `uuid`           -- String
         | FROM `system`.`databases`
         | WHERE `name`='$database'
         | """.stripMargin
    )
    val output = om.readValue[JSONOutput](result.getOutput)
    if (output.rows == 0) {
      actionIfNoSuchDatabase(database)
    }
    val row = output.data.head
    DatabaseSpec(
      name = row.get("name").asText,
      engine = row.get("engine").asText,
      data_path = row.get("data_path").asText,
      metadata_path = row.get("metadata_path").asText,
      uuid = row.get("uuid").asText
    )
  }

  def queryTableSpec(
    database: String,
    table: String,
    actionIfNoSuchTable: (String, String) => Unit = DEFAULT_ACTION_IF_NO_SUCH_TABLE
  )(implicit
    grpcNodeClient: GrpcNodeClient,
    tz: Either[ZoneId, ZoneId]
  ): TableSpec = {
    val tableResult = grpcNodeClient.syncQueryAndCheck(
      s""" SELECT
         |   `database`,                   -- String
         |   `name`,                       -- String
         |   `uuid`,                       -- UUID
         |   `engine`,                     -- String
         |   `is_temporary`,               -- UInt8
         |   `data_paths`,                 -- Array(String)
         |   `metadata_path`,              -- String
         |   `metadata_modification_time`, -- DateTime
         |   `dependencies_database`,      -- Array(String)
         |   `dependencies_table`,         -- Array(String)
         |   `create_table_query`,         -- String
         |   `engine_full`,                -- String
         |   `partition_key`,              -- String
         |   `sorting_key`,                -- String
         |   `primary_key`,                -- String
         |   `sampling_key`,               -- String
         |   `storage_policy`,             -- String
         |   `total_rows`,                 -- Nullable(UInt64)
         |   `total_bytes`,                -- Nullable(UInt64)
         |   `lifetime_rows`,              -- Nullable(UInt64)
         |   `lifetime_bytes`              -- Nullable(UInt64)
         | FROM `system`.`tables`
         | WHERE `database`='$database' AND `name`='$table'
         | """.stripMargin
    )
    val tableOutput = om.readValue[JSONOutput](tableResult.getOutput)
    if (tableOutput.rows == 0) {
      actionIfNoSuchTable(database, table)
    }
    val tableRow = tableOutput.data.head
    TableSpec(
      database = tableRow.get("database").asText,
      name = tableRow.get("name").asText,
      uuid = tableRow.get("uuid").asText,
      engine = tableRow.get("engine").asText,
      is_temporary = tableRow.get("is_temporary").asBoolean,
      data_paths = tableRow.get("data_paths").elements().asScala.map(_.asText).toArray,
      metadata_path = tableRow.get("metadata_path").asText,
      metadata_modification_time = LocalDateTime.parse(
        tableRow.get("metadata_modification_time").asText,
        dateTimeFmt.withZone(tz.merge)
      ),
      dependencies_database = tableRow.get("dependencies_database").elements().asScala.map(_.asText).toArray,
      dependencies_table = tableRow.get("dependencies_table").elements().asScala.map(_.asText).toArray,
      create_table_query = tableRow.get("create_table_query").asText,
      engine_full = tableRow.get("engine_full").asText,
      partition_key = tableRow.get("partition_key").asText,
      sorting_key = tableRow.get("sorting_key").asText,
      primary_key = tableRow.get("primary_key").asText,
      sampling_key = tableRow.get("sampling_key").asText,
      storage_policy = tableRow.get("storage_policy").asText,
      total_rows = tableRow.get("total_rows") match {
        case _: NullNode | null => None
        case node: JsonNode => Some(node.asLong)
      },
      total_bytes = tableRow.get("total_bytes") match {
        case _: NullNode | null => None
        case node: JsonNode => Some(node.asLong)
      },
      lifetime_rows = tableRow.get("lifetime_rows") match {
        case _: NullNode | null => None
        case node: JsonNode => Some(node.asLong)
      },
      lifetime_bytes = tableRow.get("lifetime_bytes") match {
        case _: NullNode | null => None
        case node: JsonNode => Some(node.asLong)
      }
    )
  }

  def queryTableSchema(
    database: String,
    table: String,
    actionIfNoSuchTable: (String, String) => Unit = DEFAULT_ACTION_IF_NO_SUCH_TABLE
  )(implicit grpcNodeClient: GrpcNodeClient): StructType = {
    val columnResult = grpcNodeClient.syncQueryAndCheck(
      s""" SELECT
         |   `database`,                -- String
         |   `table`,                   -- String
         |   `name`,                    -- String
         |   `type`,                    -- String
         |   `position`,                -- UInt64
         |   `default_kind`,            -- String
         |   `default_expression`,      -- String
         |   `data_compressed_bytes`,   -- UInt64
         |   `data_uncompressed_bytes`, -- UInt64
         |   `marks_bytes`,             -- UInt64
         |   `comment`,                 -- String
         |   `is_in_partition_key`,     -- UInt8
         |   `is_in_sorting_key`,       -- UInt8
         |   `is_in_primary_key`,       -- UInt8
         |   `is_in_sampling_key`,      -- UInt8
         |   `compression_codec`        -- String
         | FROM `system`.`columns`
         | WHERE `database`='$database' AND `table`='$table'
         | ORDER BY `position` ASC
         | """.stripMargin
    )
    val columnOutput = om.readValue[JSONOutput](columnResult.getOutput)
    if (columnOutput.rows == 0) {
      actionIfNoSuchTable(database, table)
    }
    SchemaUtil.fromClickHouseSchema(columnOutput.data.map { row =>
      val fieldName = row.get("name").asText
      val ckType = row.get("type").asText
      (fieldName, ckType)
    })
  }
}
