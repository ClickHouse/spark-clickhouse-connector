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

package xenon.clickhouse.exception

import xenon.clickhouse.spec.NodeSpec
import xenon.protocol.grpc.{Exception => GRPCException}

abstract class ClickHouseException(val code: Int, val reason: String, val node: Option[NodeSpec])
    extends RuntimeException(s"${node.getOrElse("")} [$code] $reason")

case class ClickHouseServerException(
  override val code: Int,
  override val reason: String,
  override val node: Option[NodeSpec]
) extends ClickHouseException(code, reason, node) {

  def this(exception: GRPCException, node: Option[NodeSpec] = None) =
    this(exception.getCode, exception.getDisplayText, node)
}

case class ClickHouseClientException(override val reason: String, override val node: Option[NodeSpec] = None)
    extends ClickHouseException(ClickHouseErrCode.CLIENT_ERROR.code(), reason, node)

case class RetryableClickHouseException(
  override val code: Int,
  override val reason: String,
  override val node: Option[NodeSpec]
) extends ClickHouseException(code, reason, node) {

  def this(exception: GRPCException, node: Option[NodeSpec]) = this(exception.getCode, exception.getDisplayText, node)
}
