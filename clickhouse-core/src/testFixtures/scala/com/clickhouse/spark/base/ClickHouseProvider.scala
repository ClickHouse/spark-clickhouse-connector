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

import com.clickhouse.client.ClickHouseProtocol
import com.clickhouse.client.ClickHouseProtocol.HTTP
import com.clickhouse.data.ClickHouseVersion
import com.clickhouse.spark.Utils
import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.spec.NodeSpec
import scala.collection.JavaConverters._

trait ClickHouseProvider {
  def clickhouseHost: String
  def clickhouseHttpPort: Int
  def clickhouseTcpPort: Int
  def clickhouseUser: String
  def clickhousePassword: String
  def clickhouseDatabase: String
  def clickhouseVersion: ClickHouseVersion
  def isSslEnabled: Boolean
  def isCloud: Boolean = false

  def withNodeClient(protocol: ClickHouseProtocol = HTTP)(block: NodeClient => Unit): Unit =
    Utils.tryWithResource {
      NodeClient(NodeSpec(
        clickhouseHost,
        Some(clickhouseHttpPort),
        Some(clickhouseTcpPort),
        protocol,
        username = clickhouseUser,
        database = clickhouseDatabase,
        password = clickhousePassword,
        options = Map("ssl" -> isSslEnabled.toString).asJava
      ))
    } {
      client => block(client)
    }
}
