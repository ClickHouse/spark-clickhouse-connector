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

package com.clickhouse.spark

import com.clickhouse.spark.client.NodeClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * TableProvider implementation for ClickHouse, enabling format-based access pattern:
 * {{{
 *   spark.read
 *     .format("clickhouse")
 *     .option("host", "localhost")
 *     .option("database", "default")
 *     .option("table", "my_table")
 *     .load()
 * }}}
 *
 * This implementation delegates to ClickHouseCatalog for all operations,
 * ensuring consistent behavior between catalog and format APIs.
 *
 * Particularly useful for:
 * - Ad-hoc queries without catalog configuration
 * - Databricks Unity Catalog environments
 * - Simple ETL pipelines
 * - Multi-cluster access patterns
 */
class ClickHouseTableProvider extends TableProvider
    with DataSourceRegister
    with ClickHouseHelper {

  override def shortName(): String = "clickhouse"

  /**
   * Infer schema from ClickHouse table.
   *
   * Required options:
   * - host: ClickHouse host
   * - database: Database name
   * - table: Table name
   *
   * Optional options:
   * - protocol: http (default), https, grpc
   * - http_port: HTTP port (default: 8123)
   * - user: Username (default: "default")
   * - password: Password (default: empty)
   * - ssl: Enable SSL (default: false)
   * - timezone: Timezone (default: "server")
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    log.info(s"Inferring schema for ClickHouse with options: ${options.asCaseSensitiveMap()}")

    // Infer schema from table using catalog
    val catalog = createCatalog(options)
    val identifier = extractIdentifier(options)
    try {
      val table = catalog.loadTable(identifier)
      log.info(s"Inferred schema from table with ${table.schema.fields.length} fields")
      table.schema
    } finally {
      // Catalog manages its own resources via NodeClient
    }
  }

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    log.info(s"Getting ClickHouse table with schema: ${schema.simpleString}")

    val catalog = createCatalog(options)
    val identifier = extractIdentifier(options)

    loadOrCreateTable(catalog, identifier, schema, partitioning, properties)
  }

  private def loadOrCreateTable(
    catalog: ClickHouseCatalog,
    identifier: Identifier,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table =
    try
      catalog.loadTable(identifier)
    catch {
      case _: NoSuchTableException =>
        log.info(s"Table $identifier does not exist, creating with provided schema")
        createTableWithOrderBy(catalog, identifier, schema, partitioning, properties)
    }

  private def createTableWithOrderBy(
    catalog: ClickHouseCatalog,
    identifier: Identifier,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = {
    import scala.jdk.CollectionConverters._
    val propsMap = properties.asScala.toMap
    val updatedProps = ensureOrderByConfiguration(schema, propsMap)
    catalog.createTable(identifier, schema, partitioning, updatedProps.asJava)
  }

  private def ensureOrderByConfiguration(schema: StructType, props: Map[String, String]): Map[String, String] = {
    val hasOrderBy = props.contains("order_by") || props.contains("local.order_by")

    if (!hasOrderBy) {
      addDefaultOrderBy(schema, props)
    } else {
      ensureNullableKeySupport(schema, props)
    }
  }

  private def addDefaultOrderBy(schema: StructType, props: Map[String, String]): Map[String, String] = {
    log.info(s"Schema fields: ${schema.fields.map(f => s"${f.name}(nullable=${f.nullable})").mkString(", ")}")

    val orderByColumn = schema.fields
      .find(!_.nullable)
      .map(_.name)
      .getOrElse {
        throw new IllegalArgumentException(
          "Cannot auto-generate ORDER BY clause: all columns are nullable. " +
            "ClickHouse Cloud requires non-nullable columns in ORDER BY. " +
            "Please either: (1) make at least one column non-nullable, or " +
            "(2) explicitly set 'order_by' option to specify which column(s) to use."
        )
      }

    log.info(s"Auto-generated ORDER BY clause: $orderByColumn")
    props + ("order_by" -> orderByColumn)
  }

  private def ensureNullableKeySupport(schema: StructType, props: Map[String, String]): Map[String, String] = {
    val orderByColumns = props
      .getOrElse("order_by", props.getOrElse("local.order_by", ""))
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)

    val hasNullableOrderByColumn = orderByColumns.exists { colName =>
      schema.fields.find(_.name.equalsIgnoreCase(colName)).exists(_.nullable)
    }

    if (hasNullableOrderByColumn && !props.contains("settings.allow_nullable_key")) {
      log.info("ORDER BY contains nullable columns, adding settings.allow_nullable_key=1")
      props + ("settings.allow_nullable_key" -> "1")
    } else {
      props
    }
  }

  override def supportsExternalMetadata(): Boolean = true

  private def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    val database = options.getOrDefault("database", "default")
    val table = options.get("table")

    if (table == null || table.isEmpty) {
      throw new IllegalArgumentException(
        "Required option 'table' is missing. Please provide it via .option('table', 'value')"
      )
    }

    Identifier.of(Array(database), table)
  }

  private def createCatalog(options: CaseInsensitiveStringMap): ClickHouseCatalog = {
    import scala.jdk.CollectionConverters._

    val catalog = new ClickHouseCatalog()

    val optionsMap = options.asCaseSensitiveMap().asScala.toMap
    val normalizedOptions = optionsMap ++ Map(
      Constants.CATALOG_PROP_HOST -> optionsMap.getOrElse("host", ""),
      Constants.CATALOG_PROP_DATABASE -> optionsMap.getOrElse("database", "default"),
      Constants.CATALOG_PROP_HTTP_PORT -> optionsMap.getOrElse("http_port", "8123"),
      Constants.CATALOG_PROP_PROTOCOL -> optionsMap.getOrElse("protocol", "http"),
      Constants.CATALOG_PROP_USER -> optionsMap.getOrElse("user", "default"),
      Constants.CATALOG_PROP_PASSWORD -> optionsMap.getOrElse("password", "")
    ) ++ (
      if (optionsMap.contains("ssl")) {
        Map(Constants.CATALOG_PROP_OPTION_PREFIX + "ssl" -> optionsMap("ssl"))
      } else {
        Map.empty[String, String]
      }
    )

    catalog.initialize("_temp_catalog", new CaseInsensitiveStringMap(normalizedOptions.asJava))

    catalog
  }
}
