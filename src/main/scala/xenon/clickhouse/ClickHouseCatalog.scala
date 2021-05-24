/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse

import java.time.{LocalDateTime, ZoneId}
import java.util

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.ClickHouseAnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.TransformUtil._
import xenon.clickhouse.Constants._
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.format.JSONOutput
import xenon.clickhouse.spec._

class ClickHouseCatalog extends TableCatalog with SupportsNamespaces with ClickHouseHelper with Logging {

  private var catalogName: String = _

  /////////////////////////////////////////////////////
  //////////////////// SINGLE NODE ////////////////////
  /////////////////////////////////////////////////////
  private var node: NodeSpec = _

  private var grpcNode: GrpcNodeClient = _

  // case Left  => server timezone
  // case Right => client timezone or user specific timezone
  private var tz: Either[ZoneId, ZoneId] = _

  private var currentDb: String = _

  /////////////////////////////////////////////////////
  ///////////////////// CLUSTERS //////////////////////
  /////////////////////////////////////////////////////
  private var clusters: Seq[ClusterSpec] = Nil
  private var preferLocalTable: Boolean = false

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.preferLocalTable = options.getOrDefault(CATALOG_PROP_PREFER_LOCAL_TABLE, "false").toBoolean
    this.node = buildNode(options)
    this.currentDb = node.database
    this.grpcNode = GrpcNodeClient(node)

    this.grpcNode.syncQueryAndCheck("SELECT 1")

    this.tz = options.get(CATALOG_PROP_TZ) match {
      case tz if tz == null || tz.isEmpty || tz.toLowerCase == "server" =>
        val timezoneResult = this.grpcNode.syncQueryAndCheck("SELECT timezone() AS tz")
        val timezoneOutput = om.readValue[JSONOutput](timezoneResult.getOutput)
        assert(timezoneOutput.rows == 1)
        val serverTz = ZoneId.of(timezoneOutput.data.head.get("tz").asText)
        log.info(s"detect clickhouse server timezone: $serverTz")
        Left(serverTz)
      case tz if tz.toLowerCase == "client" => Right(ZoneId.systemDefault)
      case tz => Right(ZoneId.of(tz))
    }

    val clustersResult = this.grpcNode.syncQueryAndCheck(
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
    this.clusters = om.readValue[JSONOutput](clustersResult.getOutput).data
      .groupBy(_.get("cluster").asText)
      .map { case (cluster, rows) =>
        val shards = rows
          .groupBy(_.get("shard_num").asInt)
          .map { case (shardNum, rows) =>
            val shardWeight = rows.head.get("shard_weight").asInt
            val nodes = rows.map { row =>
              val replicaNum = row.get("replica_num").asInt
              // should other properties be provided by `SparkConfs`?
              val clickhouseNode = node.copy(
                _host = row.get("host_address").asText,
                _tcp_port = Some(row.get("port").asInt)
              )
              ReplicaSpec(replicaNum, clickhouseNode)
            }
            ShardSpec(shardNum, shardWeight, nodes)
          }.toSeq
        ClusterSpec(cluster, shards)
      }.toSeq

    log.info(s"detect ${clusters.size} clickhouse clusters: ${clusters.map(_.name).mkString(",")}")
    log.info(s"clickhouse clusters details: $clusters")
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array(database) => grpcNode.syncQuery(s"SHOW TABLES IN ${quoted(database)}") match {
        case Left(exception) if exception.getCode == UNKNOWN_DATABASE.code =>
          throw new NoSuchDatabaseException(namespace.mkString("."))
        case Left(exception) =>
          throw new ClickHouseAnalysisException(exception)
        case Right(result) =>
          val output = om.readValue[JSONOutput](result.getOutput)
          output.data.map(row => row.get("name").asText()).map(table => Identifier.of(namespace, table)).toArray
      }
    case _ => throw new NoSuchDatabaseException(namespace.mkString("."))
  }

  override def loadTable(ident: Identifier): ClickHouseTable = {
    val (database, table) = unwrap(ident) match {
      case None => throw ClickHouseAnalysisException(s"Invalid table identifier: $ident")
      case Some((db, tbl)) => grpcNode.syncQuery(s"SELECT * FROM `$db`.`$tbl` WHERE 1=0") match {
          case Left(exception) if exception.getCode == UNKNOWN_TABLE.code =>
            throw new NoSuchTableException(ident.toString)
          case Left(exception) =>
            throw new ClickHouseAnalysisException(exception)
          case Right(_) => (db, tbl)
        }
    }

    val tableResult = grpcNode.syncQueryAndCheck(
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
    assert(tableOutput.rows == 1)
    val tableRow = tableOutput.data.head
    val spec = TableSpec(
      database = tableRow.get("database").asText,
      name = tableRow.get("name").asText,
      uuid = tableRow.get("uuid").asText,
      engine = tableRow.get("engine").asText,
      is_temporary = tableRow.get("is_temporary").asBoolean,
      data_paths = tableRow.get("data_paths").elements().asScala.map(_.asText).toSeq,
      metadata_path = tableRow.get("metadata_path").asText,
      metadata_modification_time = LocalDateTime.parse(
        tableRow.get("metadata_modification_time").asText,
        dateTimeFmt.withZone(tz.merge)
      ),
      dependencies_database = tableRow.get("dependencies_database").elements().asScala.map(_.asText).toSeq,
      dependencies_table = tableRow.get("dependencies_table").elements().asScala.map(_.asText).toSeq,
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

//    val tableEngineSpec = resolveTableEngine(spec, clusters)
//    resolveTableCluster(tableEngineSpec, clusters)
    new ClickHouseTable(node, None, preferLocalTable, tz, spec)
  }

  /**
   * <h2>MergeTree Engine</h2>
   * {{{
   * CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
   * (
   *     name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
   *     name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
   *     ...
   *     INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
   *     INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
   * ) ENGINE = MergeTree()
   * ORDER BY expr
   * [PARTITION BY expr]
   * [PRIMARY KEY expr]
   * [SAMPLE BY expr]
   * [TTL expr
   *     [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
   *     [WHERE conditions]
   *     [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]]
   * [SETTINGS name=value, ...]
   * }}}
   * <p>
   *
   * <h2>ReplacingMergeTree Engine</h2>
   * {{{
   * CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
   * (
   *     name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
   *     name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
   *     ...
   * ) ENGINE = ReplacingMergeTree([ver])
   * [PARTITION BY expr]
   * [ORDER BY expr]
   * [PRIMARY KEY expr]
   * [SAMPLE BY expr]
   * [SETTINGS name=value, ...]
   * }}}
   *
   * `ver` — column with version. Type `UInt*`, `Date` or `DateTime`.
   */
  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): ClickHouseTable = {
    val (db, tbl) = unwrap(ident) match {
      case Some((d, t)) => (d, t)
      case None => throw ClickHouseAnalysisException(s"Invalid table identifier: $ident")
    }
    val props = properties.asScala
    val engineExpr = props.get("engine").map(e => s"ENGINE = $e")
      .getOrElse(throw ClickHouseAnalysisException("Missing property 'engine'"))
    val partitionsExpr = partitions match {
      case transforms if transforms.nonEmpty =>
        transforms.map(toClickHouse).mkString("PARTITION BY (", ", ", ")")
      case _ => ""
    }
    val clusterExpr = props.get("cluster").map(c => s"ON CLUSTER $c").getOrElse("")
    val orderExpr = props.get("order_by").map(o => s"ORDER BY $o").getOrElse("")
    val primaryKeyExpr = props.get("primary_key").map(p => s"PRIMARY KEY $p").getOrElse("")
    val sampleExpr = props.get("sample_by").map(p => s"SAMPLE BY $p").getOrElse("")

    val settingsExpr = props.filterKeys(_.startsWith("settings.")) match {
      case settings if settings.nonEmpty =>
        settings.map { case (k, v) => s"${k.substring("settings.".length)}=$v" }.mkString("SETTINGS ", ", ", "")
      case _ => ""
    }

    val fieldsDefinition = SchemaUtil
      .toClickHouseSchema(schema)
      .map { case (fieldName, ckType) => s"${quoted(fieldName)} $ckType" }
      .mkString(",\n ")

    grpcNode.syncQueryAndCheck(
      s""" CREATE TABLE `$db`.`$tbl` $clusterExpr (
         | $fieldsDefinition
         | ) $engineExpr
         | $partitionsExpr
         | $orderExpr
         | $primaryKeyExpr
         | $sampleExpr
         | $settingsExpr
         |""".stripMargin
        .replaceAll("""\n\s+\n""", "\n")
    ) // remove empty lines

    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): ClickHouseTable =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = unwrap(ident).exists { case (db, tbl) =>
    grpcNode.syncQuery(s"DROP TABLE `$db`.`$tbl`").isRight
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    (unwrap(oldIdent), unwrap(newIdent)) match {
      case (Some((oldDb, oldTbl)), Some((newDb, newTbl))) =>
        grpcNode.syncQuery(s"RENAME TABLE `$oldDb`.`$oldTbl` to `$newDb`.`$newTbl`") match {
          case Left(exception) => throw new NoSuchTableException(exception.getDisplayText)
          case Right(_) =>
        }
      case _ => throw ClickHouseAnalysisException("invalid table identifier")
    }

  override def defaultNamespace(): Array[String] = Array(currentDb)

  override def listNamespaces(): Array[Array[String]] = {
    val result = grpcNode.syncQueryAndCheck("SHOW DATABASES")
    val output = om.readValue[JSONOutput](result.getOutput)
    output.data.map(row => Array(row.get("name").asText)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = namespace match {
    case Array() => listNamespaces()
    case Array(_) =>
      loadNamespaceMetadata(namespace)
      Array(namespace)
    case _ => throw new NoSuchNamespaceException(namespace.map(quoted).mkString("."))
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = namespace match {
    case Array(database) =>
      val result = grpcNode.syncQueryAndCheck(
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
        throw new NoSuchDatabaseException(namespace.map(quoted).mkString("."))
      }
      val row = output.data.head
      DatabaseSpec(
        name = row.get("name").asText,
        engine = row.get("engine").asText,
        data_path = row.get("data_path").asText,
        metadata_path = row.get("metadata_path").asText,
        uuid = row.get("uuid").asText
      ).toJavaMap
    case _ => throw new NoSuchDatabaseException(namespace.map(quoted).mkString("."))
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = namespace match {
    case Array(database) => grpcNode.syncQuery(s"CREATE DATABASE ${quoted(database)}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String]): Boolean = namespace match {
    case Array(database) => grpcNode.syncQuery(s"DROP DATABASE ${quoted(database)}").isRight
    case _ => false
  }
}
