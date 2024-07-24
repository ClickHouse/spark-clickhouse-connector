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

package com.clickhouse.spark.base

import com.clickhouse.data.ClickHouseVersion
import com.clickhouse.spark.Utils

trait ClickHouseCloudMixIn extends ClickHouseProvider {

  override def clickhouseHost: String = Utils.load("CLICKHOUSE_CLOUD_HOST")

  override def clickhouseHttpPort: Int = Utils.load("CLICKHOUSE_CLOUD_HTTP_PORT", "8443").toInt

  override def clickhouseTcpPort: Int = Utils.load("CLICKHOUSE_CLOUD_TCP_PORT", "9000").toInt

  override def clickhouseUser: String = Utils.load("CLICKHOUSE_CLOUD_USER", "default")

  override def clickhousePassword: String = Utils.load("CLICKHOUSE_CLOUD_PASSWORD")

  override def clickhouseDatabase: String = "default"

  override def clickhouseVersion: ClickHouseVersion = ClickHouseVersion.of("latest")

  override def isSslEnabled: Boolean = true
}
