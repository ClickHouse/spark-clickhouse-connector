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

import com.clickhouse.client.ClickHouseProtocol
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.clickhouse.SchemaUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.Constants._
import xenon.clickhouse.Utils.dateTimeFmt
import xenon.clickhouse.client.NodeClient
import xenon.clickhouse.exception.CHException
import xenon.clickhouse.spec._

import java.time.{LocalDateTime, ZoneId}
import scala.collection.JavaConverters._

trait ClickHouseHelper extends Logging {

  @volatile lazy val DEFAULT_ACTION_IF_NO_SUCH_DATABASE: String => Unit =
    (db: String) => throw NoSuchNamespaceException(db)

  @volatile lazy val DEFAULT_ACTION_IF_NO_SUCH_TABLE: (String, String) => Unit =
    (database, table) => throw NoSuchTableException(s"$database.$table")

  def unwrap(ident: Identifier): Option[(String, String)] = ident.namespace() match {
    case Array(database) => Some((database, ident.name()))
    case _ => None
  }

  def buildNodeSpec(options: CaseInsensitiveStringMap): NodeSpec = {
    val clientOpts = options.asScala
      .filterKeys(_.startsWith(CATALOG_PROP_OPTION_PREFIX))
      .filterKeys { key =>
        val clientOpt = key.substring(CATALOG_PROP_OPTION_PREFIX.length)
        val ignore = CATALOG_PROP_IGNORE_OPTIONS.contains(clientOpt)
        if (ignore) {
          log.warn(s"Ignore configuration $key.")
        }
        !ignore
      }.toMap
    NodeSpec(
      _host = options.getOrDefault(CATALOG_PROP_HOST, "localhost"),
      _grpc_port = Some(options.getInt(CATALOG_PROP_GRPC_PORT, 9100)),
      _tcp_port = Some(options.getInt(CATALOG_PROP_TCP_PORT, 9000)),
      _http_port = Some(options.getInt(CATALOG_PROP_HTTP_PORT, 8123)),
      protocol = ClickHouseProtocol.fromUriScheme(options.getOrDefault(CATALOG_PROP_PROTOCOL, "http")),
      username = options.getOrDefault(CATALOG_PROP_USER, "default"),
      password = options.getOrDefault(CATALOG_PROP_PASSWORD, ""),
      database = options.getOrDefault(CATALOG_PROP_DATABASE, "default"),
      options = clientOpts.asJava
    )
  }

  def queryClusterSpecs(nodeSpec: NodeSpec)(implicit nodeClient: NodeClient): Seq[ClusterSpec] = {
    val clustersOutput = nodeClient.syncQueryAndCheckOutputJSONEachRow(
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
    clustersOutput.records
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
                _tcp_port = Some(row.get("port").asInt),
                _grpc_port = if (Utils.isTesting) Some(9100) else nodeSpec.grpc_port,
                _http_port = if (Utils.isTesting) Some(8123) else nodeSpec.http_port
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
  )(implicit nodeClient: NodeClient): DatabaseSpec = {
    val output = nodeClient.syncQueryAndCheckOutputJSONEachRow(
      s"""SELECT
         |  `name`,          -- String
         |  `engine`,        -- String
         |  `data_path`,     -- String
         |  `metadata_path`, -- String
         |  `uuid`           -- String
         |FROM `system`.`databases`
         |WHERE `name`='$database'
         |""".stripMargin
    )
    if (output.rows == 0) {
      actionIfNoSuchDatabase(database)
    }
    val row = output.records.head
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
    nodeClient: NodeClient,
    tz: ZoneId
  ): TableSpec = {
    val tableOutput = nodeClient.syncQueryAndCheckOutputJSONEachRow(
      s"""SELECT
         |  `database`,                   -- String
         |  `name`,                       -- String
         |  `uuid`,                       -- UUID
         |  `engine`,                     -- String
         |  `is_temporary`,               -- UInt8
         |  `data_paths`,                 -- Array(String)
         |  `metadata_path`,              -- String
         |  `metadata_modification_time`, -- DateTime
         |  `dependencies_database`,      -- Array(String)
         |  `dependencies_table`,         -- Array(String)
         |  `create_table_query`,         -- String
         |  `engine_full`,                -- String
         |  `partition_key`,              -- String
         |  `sorting_key`,                -- String
         |  `primary_key`,                -- String
         |  `sampling_key`,               -- String
         |  `storage_policy`,             -- String
         |  `total_rows`,                 -- Nullable(UInt64)
         |  `total_bytes`,                -- Nullable(UInt64)
         |  `lifetime_rows`,              -- Nullable(UInt64)
         |  `lifetime_bytes`              -- Nullable(UInt64)
         |FROM `system`.`tables`
         |WHERE `database`='$database' AND `name`='$table'
         |""".stripMargin
    )
    if (tableOutput.isEmpty) {
      actionIfNoSuchTable(database, table)
    }
    val tableRow = tableOutput.records.head
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
        dateTimeFmt.withZone(tz)
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
  )(implicit nodeClient: NodeClient): StructType = {
    val columnOutput = nodeClient.syncQueryAndCheckOutputJSONEachRow(
      s"""SELECT
         |  `database`,                -- String
         |  `table`,                   -- String
         |  `name`,                    -- String
         |  `type`,                    -- String
         |  `position`,                -- UInt64
         |  `default_kind`,            -- String
         |  `default_expression`,      -- String
         |  `data_compressed_bytes`,   -- UInt64
         |  `data_uncompressed_bytes`, -- UInt64
         |  `marks_bytes`,             -- UInt64
         |  `comment`,                 -- String
         |  `is_in_partition_key`,     -- UInt8
         |  `is_in_sorting_key`,       -- UInt8
         |  `is_in_primary_key`,       -- UInt8
         |  `is_in_sampling_key`,      -- UInt8
         |  `compression_codec`        -- String
         |FROM `system`.`columns`
         |WHERE `database`='$database' AND `table`='$table'
         |ORDER BY `position` ASC
         |""".stripMargin
    )
    if (columnOutput.isEmpty) {
      actionIfNoSuchTable(database, table)
    }
    SchemaUtils.fromClickHouseSchema(columnOutput.records.map { row =>
      val fieldName = row.get("name").asText
      val ckType = row.get("type").asText
      (fieldName, ckType)
    })
  }

  def queryPartitionSpec(
    database: String,
    table: String
  )(implicit nodeClient: NodeClient): Seq[PartitionSpec] = {
    val partOutput = nodeClient.syncQueryAndCheckOutputJSONEachRow(
      s"""SELECT
         |  partition,                           -- String
         |  partition_id,                        -- String
         |  sum(rows)          AS row_count,     -- UInt64
         |  sum(bytes_on_disk) AS size_in_bytes  -- UInt64
         |FROM `system`.`parts`
         |WHERE `database`='$database' AND `table`='$table' AND `active`=1
         |GROUP BY `partition`, `partition_id`
         |ORDER BY `partition` ASC, partition_id ASC
         |""".stripMargin
    )
    if (partOutput.isEmpty || partOutput.rows == 1 && partOutput.records.head.get("partition").asText == "tuple()") {
      return Array(NoPartitionSpec)
    }
    partOutput.records.map { row =>
      PartitionSpec(
        partition_value = row.get("partition").asText,
        partition_id = row.get("partition_id").asText,
        row_count = row.get("row_count").asLong,
        size_in_bytes = row.get("size_in_bytes").asLong
      )
    }
  }

  /**
   * This method is considered as lightweight. Typically `sql` should contains `where 1=0` to avoid running the query on
   * ClickHouse.
   */
  def getQueryOutputSchema(sql: String)(implicit nodeClient: NodeClient): StructType = {
    val namesAndTypes = nodeClient.syncQueryAndCheckOutputJSONCompactEachRowWithNamesAndTypes(sql).namesAndTypes
    SchemaUtils.fromClickHouseSchema(namesAndTypes.toSeq)
  }

  def dropPartition(
    database: String,
    table: String,
    partitionExpr: String,
    cluster: Option[String] = None
  )(implicit
    nodeClient: NodeClient
  ): Boolean =
    nodeClient.syncQueryOutputJSONEachRow(
      s"ALTER TABLE `$database`.`$table` ${cluster.map(c => s"ON CLUSTER $c").getOrElse("")} DROP PARTITION $partitionExpr"
    ) match {
      case Right(_) => true
      case Left(ex: CHException) =>
        log.error(s"[${ex.code}]: ${ex.getMessage}")
        false
    }

  def delete(
    database: String,
    table: String,
    deleteExpr: String,
    cluster: Option[String] = None
  )(implicit
    nodeClient: NodeClient
  ): Boolean =
    nodeClient.syncQueryOutputJSONEachRow(
      s"ALTER TABLE `$database`.`$table` ${cluster.map(c => s"ON CLUSTER $c").getOrElse("")} DELETE WHERE $deleteExpr",
      // https://clickhouse.com/docs/en/sql-reference/statements/alter/#synchronicity-of-alter-queries
      Map("mutations_sync" -> "2")
    ) match {
      case Right(_) => true
      case Left(ex: CHException) =>
        log.error(s"[${ex.code}]: ${ex.getMessage}")
        false
    }

  def truncateTable(
    database: String,
    table: String,
    cluster: Option[String] = None
  )(implicit
    nodeClient: NodeClient
  ): Boolean = nodeClient.syncQueryOutputJSONEachRow(
    s"TRUNCATE TABLE `$database`.`$table` ${cluster.map(c => s"ON CLUSTER $c").getOrElse("")}"
  ) match {
    case Right(_) => true
    case Left(ex: CHException) =>
      log.error(s"[${ex.code}]: ${ex.getMessage}")
      false
  }
}
