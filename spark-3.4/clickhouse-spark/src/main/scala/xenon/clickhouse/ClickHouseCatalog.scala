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
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.clickhouse.{ExprUtils, SchemaUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.Constants._
import xenon.clickhouse.client.NodeClient
import xenon.clickhouse.exception.CHClientException
import xenon.clickhouse.exception.ClickHouseErrCode._
import xenon.clickhouse.func.clickhouse.ClickHouseXxHash64Shard
import xenon.clickhouse.func.{FunctionRegistry, _}
import xenon.clickhouse.spec._

import java.time.ZoneId
import java.util
import scala.collection.JavaConverters._

class ClickHouseCatalog extends TableCatalog
    with SupportsNamespaces
    with FunctionCatalog
    with ClickHouseHelper
    with SQLHelper
    with Logging {

  private var catalogName: String = _

  // ///////////////////////////////////////////////////
  // ////////////////// SINGLE NODE ////////////////////
  // ///////////////////////////////////////////////////
  private var nodeSpec: NodeSpec = _

  implicit private var nodeClient: NodeClient = _

  // case Left  => server timezone
  // case Right => client timezone or user specific timezone
  private var tz: Either[ZoneId, ZoneId] = _

  private var currentDb: String = _

  // ///////////////////////////////////////////////////
  // /////////////////// CLUSTERS //////////////////////
  // ///////////////////////////////////////////////////
  private var clusterSpecs: Seq[ClusterSpec] = Nil

  private var functionRegistry: FunctionRegistry = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.nodeSpec = buildNodeSpec(options)
    this.currentDb = nodeSpec.database
    this.nodeClient = NodeClient(nodeSpec)

    this.nodeClient.syncQueryAndCheckOutputJSONEachRow("SELECT 1")

    this.tz = options.get(CATALOG_PROP_TZ) match {
      case tz if tz == null || tz.isEmpty || tz.toLowerCase == "server" =>
        val timezoneOutput = this.nodeClient.syncQueryAndCheckOutputJSONEachRow("SELECT timezone() AS tz")
        assert(timezoneOutput.rows == 1)
        val serverTz = ZoneId.of(timezoneOutput.records.head.get("tz").asText)
        log.info(s"Detect ClickHouse server timezone: $serverTz")
        Left(serverTz)
      case tz if tz.toLowerCase == "client" => Right(ZoneId.systemDefault)
      case tz => Right(ZoneId.of(tz))
    }

    this.clusterSpecs = queryClusterSpecs(nodeSpec)

    val dynamicFunctionRegistry = new DynamicFunctionRegistry
    val xxHash64ShardFunc = new ClickHouseXxHash64Shard(clusterSpecs)
    dynamicFunctionRegistry.register("ck_xx_hash64_shard", xxHash64ShardFunc) // for compatible
    dynamicFunctionRegistry.register("clickhouse_shard_xxHash64", xxHash64ShardFunc)
    this.functionRegistry = new CompositeFunctionRegistry(Array(StaticFunctionRegistry, dynamicFunctionRegistry))

    log.info(s"Detect ${clusterSpecs.size} ClickHouse clusters: ${clusterSpecs.map(_.name).mkString(",")}")
    log.info(s"ClickHouse clusters' detail: $clusterSpecs")
    log.info(s"functionRegistry: ${this.functionRegistry.list.mkString(",")}")
  }

  override def name(): String = catalogName

  @throws[NoSuchNamespaceException]
  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array(database) =>
      nodeClient.syncQueryOutputJSONEachRow(s"SHOW TABLES IN ${quoted(database)}") match {
        case Left(exception) if exception.code == UNKNOWN_DATABASE.code =>
          throw new NoSuchNamespaceException(namespace.mkString("."))
        case Left(rethrow) =>
          throw rethrow
        case Right(output) =>
          output.records
            .map(row => row.get("name").asText)
            .map(table => Identifier.of(namespace, table))
            .toArray
      }
    case _ => throw new NoSuchNamespaceException(namespace.mkString("."))
  }

  @throws[NoSuchTableException]
  override def loadTable(ident: Identifier): ClickHouseTable = {
    val (database, table) = unwrap(ident) match {
      case None => throw new NoSuchTableException(ident)
      case Some((db, tbl)) =>
        nodeClient.syncQueryOutputJSONEachRow(s"SELECT * FROM `$db`.`$tbl` WHERE 1=0") match {
          case Left(exception) if exception.code == UNKNOWN_TABLE.code =>
            throw new NoSuchTableException(ident)
          // not sure if this check is necessary
          case Left(exception) if exception.code == UNKNOWN_DATABASE.code =>
            throw new NoSuchTableException(s"Database $db does not exist")
          case Left(rethrow) =>
            throw rethrow
          case Right(_) => (db, tbl)
        }
    }
    implicit val _tz: ZoneId = tz.merge
    val tableSpec = queryTableSpec(database, table)
    val tableEngineSpec = TableEngineUtils.resolveTableEngine(tableSpec)
    val tableClusterSpec = tableEngineSpec match {
      case distributeSpec: DistributedEngineSpec =>
        Some(TableEngineUtils.resolveTableCluster(distributeSpec, clusterSpecs))
      case _ => None
    }
    ClickHouseTable(
      nodeSpec,
      tableClusterSpec,
      _tz,
      tableSpec,
      tableEngineSpec,
      functionRegistry
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
   *     [GROUP BY key_expr [SET v1 = agg_func(v1) [, v2 = agg_func(v2) ...]] ]]
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
  @throws[TableAlreadyExistsException]
  @throws[NoSuchNamespaceException]
  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): ClickHouseTable = {
    val (db, tbl) = unwrap(ident) match {
      case Some((d, t)) => (d, t)
      case None => throw CHClientException(s"Invalid table identifier: $ident")
    }
    val props = properties.asScala

    val engineExpr = props.getOrElse("engine", "MergeTree()")

    val isCreatingDistributed = engineExpr equalsIgnoreCase "Distributed"
    val keyPrefix = if (isCreatingDistributed) "local." else ""

    val partitionsClause = partitions match {
      case transforms if transforms.nonEmpty =>
        transforms.map(ExprUtils.toClickHouse(_)(functionRegistry).sql).mkString("PARTITION BY (", ", ", ")")
      case _ => ""
    }

    val orderClause = props.get(s"${keyPrefix}order_by").map(o => s"ORDER BY ($o)").getOrElse("")
    val primaryKeyClause = props.get(s"${keyPrefix}primary_key").map(p => s"PRIMARY KEY ($p)").getOrElse("")
    val sampleClause = props.get(s"${keyPrefix}sample_by").map(p => s"SAMPLE BY ($p)").getOrElse("")

    val fieldsClause = SchemaUtils
      .toClickHouseSchema(schema)
      .map { case (fieldName, ckType, comment) => s"${quoted(fieldName)} $ckType$comment" }
      .mkString(",\n ")

    val clusterOpt = props.get("cluster")

    def tblSettingsClause(prefix: String): String = props.filterKeys(_.startsWith(prefix)) match {
      case settings if settings.nonEmpty =>
        settings.map { case (k, v) =>
          s"${k.substring(prefix.length)}=$v"
        }.mkString("SETTINGS ", ", ", "")
      case _ => ""
    }

    def createTable(
      clusterOpt: Option[String],
      engineExpr: String,
      database: String,
      table: String,
      settingsClause: String
    ): Unit = {
      val clusterClause = clusterOpt.map(c => s"ON CLUSTER $c").getOrElse("")
      nodeClient.syncQueryAndCheckOutputJSONEachRow(
        s"""CREATE TABLE `$database`.`$table` $clusterClause (
           |$fieldsClause
           |) ENGINE = $engineExpr
           |$partitionsClause
           |$orderClause
           |$primaryKeyClause
           |$sampleClause
           |$settingsClause
           |""".stripMargin
          .replaceAll("""\n\s+\n""", "\n") // remove empty lines
      )
    }

    def createDistributedTable(
      cluster: String,
      shardExpr: String,
      localDatabase: String,
      localTable: String,
      distributedDatabase: String,
      distributedTable: String,
      settingsClause: String
    ): Unit = nodeClient.syncQueryAndCheckOutputJSONEachRow(
      s"""CREATE TABLE `$distributedDatabase`.`$distributedTable` ON CLUSTER $cluster
         |AS `$localDatabase`.`$localTable`
         |ENGINE = Distributed($cluster, '$localDatabase', '$localTable', ($shardExpr))
         |$settingsClause
         |""".stripMargin
    )

    if (isCreatingDistributed) {
      val cluster = clusterOpt.getOrElse("default")
      val shardExpr = props.getOrElse("shard_by", "rand()")
      val settingsClause = tblSettingsClause("settings.")
      val localEngineExpr = props.getOrElse(s"${keyPrefix}engine", s"MergeTree()")
      val localDatabase = props.getOrElse(s"${keyPrefix}database", db)
      val localTable = props.getOrElse(s"${keyPrefix}table", s"${tbl}_local")
      val localSettingsClause = tblSettingsClause(s"${keyPrefix}settings.")
      createTable(Some(cluster), localEngineExpr, localDatabase, localTable, localSettingsClause)
      createDistributedTable(cluster, shardExpr, localDatabase, localTable, db, tbl, settingsClause)
    } else {
      val settingsClause = tblSettingsClause(s"${keyPrefix}settings.")
      createTable(clusterOpt, engineExpr, db, tbl, settingsClause)
    }

    loadTable(ident)
  }

  @throws[NoSuchTableException]
  override def alterTable(ident: Identifier, changes: TableChange*): ClickHouseTable =
    throw new UnsupportedOperationException

  override def dropTable(ident: Identifier): Boolean = {
    val tableOpt =
      try Some(loadTable(ident))
      catch {
        case _: NoSuchTableException => None
      }
    tableOpt match {
      case None => false
      case Some(ClickHouseTable(_, cluster, _, tableSpec, _, _)) =>
        val (db, tbl) = (tableSpec.database, tableSpec.name)
        val isAtomic = loadNamespaceMetadata(Array(db)).get("engine").equalsIgnoreCase("atomic")
        val syncClause = if (isAtomic) "SYNC" else ""
        // limitation: only support Distribute table, can not handle cases such as drop local table on cluster nodes
        val clusterClause = cluster.map(c => s"ON CLUSTER ${c.name}").getOrElse("")
        nodeClient.syncQueryOutputJSONEachRow(s"DROP TABLE `$db`.`$tbl` $clusterClause $syncClause").isRight
    }
  }

  @throws[NoSuchTableException]
  @throws[TableAlreadyExistsException]
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    (unwrap(oldIdent), unwrap(newIdent)) match {
      case (Some((oldDb, oldTbl)), Some((newDb, newTbl))) =>
        nodeClient.syncQueryOutputJSONEachRow(s"RENAME TABLE `$oldDb`.`$oldTbl` to `$newDb`.`$newTbl`") match {
          case Left(exception) => throw new NoSuchTableException(exception.getMessage, Some(exception))
          case Right(_) =>
        }
      case _ => throw CHClientException("Invalid table identifier")
    }

  override def defaultNamespace(): Array[String] = Array(currentDb)

  @throws[NoSuchNamespaceException]
  override def listNamespaces(): Array[Array[String]] = {
    val output = nodeClient.syncQueryAndCheckOutputJSONEachRow("SHOW DATABASES")
    output.records.map(row => Array(row.get("name").asText)).toArray
  }

  @throws[NoSuchNamespaceException]
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = namespace match {
    case Array() => listNamespaces()
    case Array(_) =>
      loadNamespaceMetadata(namespace)
      Array()
    case _ => throw new NoSuchNamespaceException(namespace.map(quoted).mkString("."))
  }

  @throws[NoSuchNamespaceException]
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = namespace match {
    case Array(database) => queryDatabaseSpec(database).toJavaMap
    case _ => throw new NoSuchNamespaceException(namespace.map(quoted).mkString("."))
  }

  @throws[NamespaceAlreadyExistsException]
  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = namespace match {
    case Array(database) =>
      val onClusterClause = metadata.asScala.get("cluster").map(c => s"ON CLUSTER $c").getOrElse("")
      nodeClient.syncQueryOutputJSONEachRow(s"CREATE DATABASE ${quoted(database)} $onClusterClause")
  }

  @throws[NoSuchNamespaceException]
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    throw new UnsupportedOperationException("ALTER NAMESPACE OPERATION is unsupported yet")

  @throws[NoSuchNamespaceException]
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = namespace match {
    case Array(database) =>
      loadNamespaceMetadata(namespace) // test existing
      if (!cascade && listNamespaces(namespace).nonEmpty) {
        throw new NonEmptyNamespaceException(namespace)
      }
      nodeClient.syncQueryOutputJSONEachRow(s"DROP DATABASE ${quoted(database)}").isRight
    case _ => false
  }

  @throws[NoSuchNamespaceException]
  override def listFunctions(namespace: Array[String]): Array[Identifier] =
    functionRegistry.list.map(name => Identifier.of(Array.empty, name))

  @throws[NoSuchFunctionException]
  override def loadFunction(ident: Identifier): UnboundFunction =
    functionRegistry.load(ident.name).getOrElse(throw new NoSuchFunctionException(ident))
}
