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

import java.time.ZoneId
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.clickhouse.ClickHouseAnalysisException
import org.apache.spark.sql.clickhouse.util.TransformUtil._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.Constants._
import xenon.clickhouse.Utils._
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.format.JSONOutput
import xenon.clickhouse.spec._

class ClickHouseCatalog extends TableCatalog with SupportsNamespaces
    with ClickHouseHelper with SQLHelper with Logging {

  private var catalogName: String = _

  /////////////////////////////////////////////////////
  //////////////////// SINGLE NODE ////////////////////
  /////////////////////////////////////////////////////
  private var nodeSpec: NodeSpec = _

  implicit private var grpcNodeClient: GrpcNodeClient = _

  // case Left  => server timezone
  // case Right => client timezone or user specific timezone
  private var tz: Either[ZoneId, ZoneId] = _

  private var currentDb: String = _

  /////////////////////////////////////////////////////
  ///////////////////// CLUSTERS //////////////////////
  /////////////////////////////////////////////////////
  private var clusterSpecs: Seq[ClusterSpec] = Nil

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.nodeSpec = buildNodeSpec(options)
    this.currentDb = nodeSpec.database
    this.grpcNodeClient = GrpcNodeClient(nodeSpec)

    this.grpcNodeClient.syncQueryAndCheck("SELECT 1")

    this.tz = options.get(CATALOG_PROP_TZ) match {
      case tz if tz == null || tz.isEmpty || tz.toLowerCase == "server" =>
        val timezoneResult = this.grpcNodeClient.syncQueryAndCheck("SELECT timezone() AS tz")
        val timezoneOutput = om.readValue[JSONOutput](timezoneResult.getOutput)
        assert(timezoneOutput.rows == 1)
        val serverTz = ZoneId.of(timezoneOutput.data.head.get("tz").asText)
        log.info(s"detect clickhouse server timezone: $serverTz")
        Left(serverTz)
      case tz if tz.toLowerCase == "client" => Right(ZoneId.systemDefault)
      case tz => Right(ZoneId.of(tz))
    }

    this.clusterSpecs = queryClusterSpecs(nodeSpec)

    log.info(s"detect ${clusterSpecs.size} clickhouse clusters: ${clusterSpecs.map(_.name).mkString(",")}")
    log.info(s"clickhouse clusters details: $clusterSpecs")
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array(database) => grpcNodeClient.syncQuery(s"SHOW TABLES IN ${quoted(database)}") match {
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
      case Some((db, tbl)) => grpcNodeClient.syncQuery(s"SELECT * FROM `$db`.`$tbl` WHERE 1=0") match {
          case Left(exception) if exception.getCode == UNKNOWN_TABLE.code =>
            throw new NoSuchTableException(ident.toString)
          case Left(exception) =>
            throw new ClickHouseAnalysisException(exception)
          case Right(_) => (db, tbl)
        }
    }
    implicit val _tz: ZoneId = tz.merge
    val tableSpec = queryTableSpec(database, table)
    val tableEngineSpec = TableEngineUtil.resolveTableEngine(tableSpec)
    val tableClusterSpec = tableEngineSpec match {
      case distributeSpec: DistributedEngineSpec =>
        Some(TableEngineUtil.resolveTableCluster(distributeSpec, clusterSpecs))
      case _ => None
    }
    new ClickHouseTable(
      nodeSpec,
      tableClusterSpec,
      _tz,
      tableSpec,
      tableEngineSpec
    )
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
   *
   * TODO Support create Distributed tables
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
    // TODO we need consider to support other DML, like alter table, drop table, truncate table ...
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

    grpcNodeClient.syncQueryAndCheck(
      s""" CREATE TABLE `$db`.`$tbl` $clusterExpr (
         | $fieldsDefinition
         | ) $engineExpr
         | $partitionsExpr
         | $orderExpr
         | $primaryKeyExpr
         | $sampleExpr
         | $settingsExpr
         |""".stripMargin
        .replaceAll("""\n\s+\n""", "\n") // remove empty lines
    )

    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): ClickHouseTable =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = unwrap(ident).exists { case (db, tbl) =>
    grpcNodeClient.syncQuery(s"DROP TABLE `$db`.`$tbl`").isRight
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    (unwrap(oldIdent), unwrap(newIdent)) match {
      case (Some((oldDb, oldTbl)), Some((newDb, newTbl))) =>
        grpcNodeClient.syncQuery(s"RENAME TABLE `$oldDb`.`$oldTbl` to `$newDb`.`$newTbl`") match {
          case Left(exception) => throw new NoSuchTableException(exception.getDisplayText)
          case Right(_) =>
        }
      case _ => throw ClickHouseAnalysisException("invalid table identifier")
    }

  override def defaultNamespace(): Array[String] = Array(currentDb)

  override def listNamespaces(): Array[Array[String]] = {
    val result = grpcNodeClient.syncQueryAndCheck("SHOW DATABASES")
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
    case Array(database) => queryDatabaseSpec(database).toJavaMap
    case _ => throw new NoSuchDatabaseException(namespace.map(quoted).mkString("."))
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = namespace match {
    case Array(database) => grpcNodeClient.syncQuery(s"CREATE DATABASE ${quoted(database)}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String]): Boolean = namespace match {
    case Array(database) => grpcNodeClient.syncQuery(s"DROP DATABASE ${quoted(database)}").isRight
    case _ => false
  }
}
