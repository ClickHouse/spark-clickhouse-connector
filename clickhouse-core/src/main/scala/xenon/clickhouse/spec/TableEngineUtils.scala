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

package xenon.clickhouse.spec

import xenon.clickhouse.exception.CHClientException
import xenon.clickhouse.parse.{ParseException, ParseUtils}
import xenon.clickhouse.Logging

object TableEngineUtils extends Logging {

  def resolveTableEngine(tableSpec: TableSpec): TableEngineSpec = synchronized {
    try ParseUtils.parser.parseEngineClause(tableSpec.engine_full)
    catch {
      case cause: ParseException =>
        log.warn(s"Unknown table engine for table ${tableSpec.database}.${tableSpec.name}: ${tableSpec.engine_full}")
        UnknownTableEngineSpec(tableSpec.engine_full)
    }
  }

  def resolveTableCluster(distributedEngineSpec: DistributedEngineSpec, clusterSpecs: Seq[ClusterSpec]): ClusterSpec =
    clusterSpecs.find(_.name == distributedEngineSpec.cluster)
      .getOrElse(throw CHClientException(s"Unknown cluster: ${distributedEngineSpec.cluster}"))
}
