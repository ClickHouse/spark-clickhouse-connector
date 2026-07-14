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

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseProvider}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.tags.Cloud

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

/**
 * Confirms that SSL connectivity works end-to-end against a real ClickHouse server.
 *
 * Related to https://github.com/ClickHouse/spark-clickhouse-connector/issues/557 : the connector
 * uses `option.ssl` to select the `https://` URL scheme and (today) also forwards it into the
 * clickhouse-java client. This suite is the connectivity guard for that behaviour — it proves that
 * with `option.ssl = true` the connector actually talks to the server over TLS and can both read and
 * write data. Any future change that stops leaking `ssl` into the java client must keep this passing.
 *
 * Runs only against a real, SSL-enabled ClickHouse (ClickHouse Cloud). It is tagged `@Cloud`, so it
 * is excluded from the default `test` task and executed via the `cloudTest` task with the
 * `CLICKHOUSE_CLOUD_HOST` / `CLICKHOUSE_CLOUD_PASSWORD` (and optionally `CLICKHOUSE_CLOUD_USER`,
 * `CLICKHOUSE_CLOUD_HTTP_PORT`) environment variables set:
 *
 * {{{
 *   CLICKHOUSE_CLOUD_HOST=<host> CLICKHOUSE_CLOUD_PASSWORD=<pwd> \
 *     ./gradlew :clickhouse-core-it:cloudTest \
 *     --tests "com.clickhouse.spark.ClickHouseCloudSslConnectivitySuite"
 * }}}
 */
@Cloud
class ClickHouseCloudSslConnectivitySuite extends SslConnectivitySuite with ClickHouseCloudMixIn

abstract class SslConnectivitySuite extends AnyFunSuite with ClickHouseProvider with Logging {

  test("SSL connection works end-to-end (query + insert round-trip over https)") {
    assert(isSslEnabled, "This suite must run against an SSL-enabled ClickHouse server")

    withNodeClient() { client =>
      // 1. Basic connectivity: a successful query against a Cloud host (which only serves HTTPS on
      //    the configured port) already proves the TLS handshake and request succeeded.
      val ping = client.syncQueryAndCheckOutputJSONEachRow("SELECT 1 AS one")
      assert(ping.rows === 1L)
      assert(ping.records.head.get("one").asInt === 1)

      // 2. Write path over SSL (the scenario from issue #557): create, insert, read back, drop.
      val db = clickhouseDatabase
      val table = "sccs_ssl_roundtrip_it"

      client.syncQueryAndCheckOutputJSONEachRow(s"DROP TABLE IF EXISTS `$db`.`$table`")
      client.syncQueryAndCheckOutputJSONEachRow(
        s"CREATE TABLE `$db`.`$table` (id Int32, name String) ENGINE = MergeTree ORDER BY id"
      )
      try {
        val payload =
          """{"id":1,"name":"ssl"}
            |{"id":2,"name":"works"}
            |""".stripMargin

        client.syncInsertOutputJSONEachRow(
          db,
          table,
          "JSONEachRow",
          new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8))
        ) match {
          case Right(_) => // insert succeeded over SSL
          case Left(ex) => fail(s"insert over SSL failed: ${ex.getMessage}", ex)
        }

        val readBack = client.syncQueryAndCheckOutputJSONEachRow(
          s"SELECT count() AS cnt FROM `$db`.`$table`"
        )
        assert(readBack.records.head.get("cnt").asInt === 2)
      } finally {
        client.syncQueryAndCheckOutputJSONEachRow(s"DROP TABLE IF EXISTS `$db`.`$table`")
      }
    }
  }
}
