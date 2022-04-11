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

import scala.util.Using

import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.grpc.GrpcNodeClient

class ClickHouseCommandRunner extends ExternalCommandRunner with ClickHouseHelper {

  override def executeCommand(sql: String, options: CaseInsensitiveStringMap): Array[String] =
    Using.resource(GrpcNodeClient(buildNodeSpec(options))) { grpcNodeClient =>
      grpcNodeClient.syncQueryAndCheckOutputJSONEachRow(sql).records.map(_.toString).toArray
    }
}
