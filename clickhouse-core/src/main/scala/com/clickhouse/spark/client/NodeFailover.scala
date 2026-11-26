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

package com.clickhouse.spark.client

import com.clickhouse.spark.Logging
import com.clickhouse.spark.exception.{CHClientException, CHException, RetryableCHException}
import com.clickhouse.spark.spec.NodeSpec

import java.io.IOException
import java.net.{
  ConnectException,
  NoRouteToHostException,
  SocketException,
  SocketTimeoutException,
  UnknownHostException
}
import java.util.concurrent.{ExecutionException, TimeoutException}
import scala.annotation.tailrec
import scala.util.control.NonFatal

object NodeFailover extends Logging {

  /**
   * True when the failure indicates the endpoint itself is unreachable / unusable, so another
   * replica of the same shard should be tried.
   */
  def isFailoverWorthy(throwable: Throwable): Boolean = throwable match {
    case null => false
    case _: RetryableCHException => true
    case _: CHClientException => true
    case _: ConnectException => true
    case _: SocketTimeoutException => true
    case _: UnknownHostException => true
    case _: SocketException => true
    case _: TimeoutException => true
    case _: IOException => true
    case e: ExecutionException => isFailoverWorthy(e.getCause)
    case e if e.getCause != null && (e.getCause ne e) => isFailoverWorthy(e.getCause)
    case _ => false
  }

  def isFailoverWorthy(exception: CHException): Boolean = exception match {
    case _: RetryableCHException => true
    case _: CHClientException => true
    case other => isFailoverWorthy(other.getCause)
  }

  /**
   * Run `action` against successive live candidates. Opens a fresh [[NodeClient]] per attempt and
   * always closes it. On a failover-worthy failure the node is marked dead and the next candidate
   * is tried. Non-failover failures are rethrown immediately.
   */
  def withFailover[R](candidates: Seq[NodeSpec], queryTimeoutMs: Long)(action: NodeClient => R): R = {
    val live = NodeHealthTracker.live(candidates)
    if (live.isEmpty) {
      throw CHClientException(
        s"No live ClickHouse replicas among ${candidates.map(_.toString).mkString("[", ", ", "]")}"
      )
    }
    go(live, queryTimeoutMs, action, lastFailure = None)
  }

  @tailrec
  private def go[R](
    remaining: Seq[NodeSpec],
    queryTimeoutMs: Long,
    action: NodeClient => R,
    lastFailure: Option[Throwable]
  ): R = remaining match {
    case Nil =>
      throw lastFailure.getOrElse(CHClientException("No live ClickHouse replicas remaining"))
    case head +: rest =>
      val client = NodeClient(head, queryTimeoutMs)
      val outcome =
        try
          Right(action(client))
        catch {
          case NonFatal(t) => Left(t)
        } finally
          try client.close()
          catch { case NonFatal(_) => () }
      outcome match {
        case Right(result) => result
        case Left(t) if isFailoverWorthy(t) && rest.nonEmpty =>
          log.warn(s"Failover from $head to next replica after: ${t.getMessage}")
          NodeHealthTracker.markDead(head)
          go(rest, queryTimeoutMs, action, Some(t))
        case Left(t) if isFailoverWorthy(t) =>
          NodeHealthTracker.markDead(head)
          throw t
        case Left(t) =>
          throw t
      }
  }
}
