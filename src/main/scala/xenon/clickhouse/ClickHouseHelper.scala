package xenon.clickhouse

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.spec.NodeSpec
import xenon.clickhouse.Constants._

import java.time.format.DateTimeFormatter

trait ClickHouseHelper {

  def quoted(token: String) = s"`$token`"

  def unwrap(ident: Identifier): Option[(String, String)] = ident.namespace() match {
    case Array(database) => Some((database, ident.name()))
    case _ => None
  }

  def buildNode(options: CaseInsensitiveStringMap): NodeSpec = {
    val host = options.getOrDefault(CATALOG_PROP_HOST, "localhost")
    val port = options.getInt(CATALOG_PROP_PORT, 9100)
    val user = options.getOrDefault(CATALOG_PROP_USER, "default")
    val password = options.getOrDefault(CATALOG_PROP_PASSWORD, "")
    val database = options.getOrDefault(CATALOG_PROP_DATABASE, "default")
    NodeSpec(_host = host, _grpc_port = Some(port), username = user, password = password, database = database)
  }
}
