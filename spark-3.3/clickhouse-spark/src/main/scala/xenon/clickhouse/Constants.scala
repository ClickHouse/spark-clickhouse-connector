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

import com.clickhouse.client.config.ClickHouseClientOption._

object Constants {
  // format: off
  //////////////////////////////////////////////////////////
  //////// clickhouse datasource catalog properties ////////
  //////////////////////////////////////////////////////////
  final val CATALOG_PROP_HOST           = "host"
  final val CATALOG_PROP_GRPC_PORT      = "grpc_port"
  final val CATALOG_PROP_TCP_PORT       = "tcp_port"
  final val CATALOG_PROP_HTTP_PORT      = "http_port"
  final val CATALOG_PROP_PROTOCOL       = "protocol"
  final val CATALOG_PROP_USER           = "user"
  final val CATALOG_PROP_PASSWORD       = "password"
  final val CATALOG_PROP_DATABASE       = "database"
  final val CATALOG_PROP_TZ             = "timezone" // server(default), client, UTC+3, Asia/Shanghai, etc.
  final val CATALOG_PROP_OPTION_PREFIX  = "option."
  final val CATALOG_PROP_IGNORE_OPTIONS = Seq(
    DATABASE.getKey, COMPRESS.getKey, DECOMPRESS.getKey, FORMAT.getKey, RETRY.getKey,
    USE_SERVER_TIME_ZONE.getKey, USE_SERVER_TIME_ZONE_FOR_DATES.getKey, SERVER_TIME_ZONE.getKey, USE_TIME_ZONE.getKey)

  //////////////////////////////////////////////////////////
  ////////// clickhouse datasource read properties /////////
  //////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  ///////// clickhouse datasource write properties /////////
  //////////////////////////////////////////////////////////
  // format: on
}
