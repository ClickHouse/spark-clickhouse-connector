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

import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.clickhouse.spark.client.NodeClient

class ClickHouseCommandRunner extends ExternalCommandRunner with ClickHouseHelper {

  override def executeCommand(sql: String, options: CaseInsensitiveStringMap): Array[String] =
    Utils.tryWithResource(NodeClient(buildNodeSpec(options))) { nodeClient =>
      nodeClient.syncQueryAndCheckOutputJSONEachRow(sql).records.map(_.toString).toArray
    }
}
