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

package com.clickhouse.spark.exception

import com.clickhouse.spark.spec.NodeSpec

abstract class CHException(
  val code: Int,
  val reason: String,
  val node: Option[NodeSpec],
  val cause: Option[Throwable]
) extends RuntimeException(s"${node.getOrElse("")} [$code] $reason", cause.orNull)

case class CHServerException(
  override val code: Int,
  override val reason: String,
  override val node: Option[NodeSpec],
  override val cause: Option[Throwable]
) extends CHException(code, reason, node, cause)

case class CHClientException(
  override val reason: String,
  override val node: Option[NodeSpec] = None,
  override val cause: Option[Throwable] = None
) extends CHException(ClickHouseErrCode.CLIENT_ERROR.code(), reason, node, cause)

case class RetryableCHException(
  override val code: Int,
  override val reason: String,
  override val node: Option[NodeSpec],
  override val cause: Option[Throwable] = None
) extends CHException(code, reason, node, cause)
