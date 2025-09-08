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

package com.clickhouse.spark.spec

import com.clickhouse.spark.Logging
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.parse.{ParseException, ParseUtils}

object TableEngineUtils extends Logging {

  def resolveTableEngine(tableSpec: TableSpec): TableEngineSpec = synchronized {
    try ParseUtils.parser.parseEngineClause(tableSpec.engine_full)
    catch {
      case cause: ParseException =>
        log.warn(s"Unknown table engine for table ${tableSpec.database}.${tableSpec.name}: ${tableSpec.engine_full}")
        UnknownTableEngineSpec(tableSpec.engine_full)
    }
  }

  def resolveTableCluster(
    distributedEngineSpec: DistributedEngineSpec,
    clusterSpecs: Seq[ClusterSpec],
    macrosSpecs: Seq[MacrosSpec]
  ): ClusterSpec = {
    val clusterName = if (distributedEngineSpec.cluster.contains("{")) {
      val macrosMap = macrosSpecs.map(spec => (spec.name, spec.substitution)).toMap

      var resolvedClusterName = distributedEngineSpec.cluster
      var startPos = resolvedClusterName.indexOf('{')
      while (startPos >= 0) {
        val endPos = resolvedClusterName.indexOf('}', startPos)
        if (endPos > startPos) {
          val macroName = resolvedClusterName.substring(startPos + 1, endPos)
          val substitution = macrosMap.getOrElse(macroName, throw CHClientException(s"Unknown macro: ${macroName}"))
          resolvedClusterName = resolvedClusterName
            .substring(0, startPos)
            .concat(substitution)
            .concat(resolvedClusterName.substring(endPos + 1))
        }
        startPos = resolvedClusterName.indexOf('{')
      }
      resolvedClusterName
    } else {
      distributedEngineSpec.cluster
    }

    clusterSpecs.find(_.name == clusterName)
      .getOrElse(throw CHClientException(s"Unknown cluster: resolved name '${clusterName}' (original: '${distributedEngineSpec.cluster}')"))
  }
}
