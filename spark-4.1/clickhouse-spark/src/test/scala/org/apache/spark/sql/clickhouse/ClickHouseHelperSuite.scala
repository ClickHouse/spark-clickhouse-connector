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

package org.apache.spark.sql.clickhouse

import com.clickhouse.spark.{ClickHouseHelper, Utils}
import com.clickhouse.spark.Constants._
import com.clickhouse.spark.client.NodeClient
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.CLIENT_QUERY_TIMEOUT
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class ClickHouseHelperSuite extends AnyFunSuite with ClickHouseHelper {

  test("buildNodeSpec") {
    val nodeSpec = buildNodeSpec(
      new CaseInsensitiveStringMap(Map(
        "database" -> "testing",
        "option.database" -> "production",
        "option.use_time_zone" -> "Asia/Shanghai",
        "option.ssl" -> "true"
      ).asJava)
    )
    assert(nodeSpec.database === "testing")
    assert(!nodeSpec.options.containsKey("use_time_zone"))
    assert(nodeSpec.ssl)
    assert(!nodeSpec.options.containsKey("ssl"))
  }

  test("no known catalog property is forwarded to the ClickHouse client") {
    // Connector properties the client knows nothing about must be consumed here, or the client warns
    // about them (issue #557). Keep this list in step with the catalog properties in `Constants`.
    val catalogProps = Seq(
      CATALOG_PROP_HOST -> "localhost",
      CATALOG_PROP_TCP_PORT -> "9000",
      CATALOG_PROP_HTTP_PORT -> "8123",
      CATALOG_PROP_PROTOCOL -> "http",
      CATALOG_PROP_USER -> "default",
      CATALOG_PROP_PASSWORD -> "",
      CATALOG_PROP_DATABASE -> "default",
      CATALOG_PROP_TZ -> "server",
      CATALOG_INFER_RUNTIME_ENV -> "false",
      CATALOG_PROP_SSL -> "true",
      CATALOG_PROP_OPTION_PREFIX + CATALOG_PROP_SSL -> "true"
    )
    val nodeSpec = buildNodeSpec(new CaseInsensitiveStringMap(catalogProps.toMap.asJava))

    assert(
      nodeSpec.options.isEmpty,
      s"these catalog properties would reach the client: ${nodeSpec.options.keySet().asScala.toSeq.sorted}"
    )
    // Building the client parses the config map, so an unusable option would fail here.
    Utils.tryWithResource(NodeClient(nodeSpec))(_ => ())
  }

  test("catalog properties the connector ignores are not forwarded either") {
    val ignored = CATALOG_PROP_IGNORE_OPTIONS.map(key => CATALOG_PROP_OPTION_PREFIX + key -> "1")
    val nodeSpec = buildNodeSpec(new CaseInsensitiveStringMap(ignored.toMap.asJava))

    assert(nodeSpec.options.isEmpty)
    Utils.tryWithResource(NodeClient(nodeSpec))(_ => ())
  }

  test("catalog timezone uses ClickHouse client option as fallback") {
    val options = new CaseInsensitiveStringMap(Map(
      "option.use_time_zone" -> "Asia/Shanghai"
    ).asJava)

    assert(catalogTimeZone(options) === "Asia/Shanghai")
  }

  test("catalog timezone takes precedence over ClickHouse client option") {
    val options = new CaseInsensitiveStringMap(Map(
      "timezone" -> "client",
      "option.use_time_zone" -> "Asia/Shanghai"
    ).asJava)

    assert(catalogTimeZone(options) === "client")
  }

  test("client query timeout uses SQLConf") {
    val conf = SQLConf.get
    val original = conf.getConf(CLIENT_QUERY_TIMEOUT)
    assert(original === 60000L)
    try {
      conf.setConfString(CLIENT_QUERY_TIMEOUT.key, "1234ms")
      assert(clientQueryTimeoutMs === 1234L)
    } finally
      conf.setConfString(CLIENT_QUERY_TIMEOUT.key, s"${original}ms")
  }
}
