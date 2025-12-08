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

import scala.util.{Failure, Success, Try}

object RetryUtils {

  def retryOnReplicationError[T](
    operation: => T,
    operationName: String,
    maxRetries: Int = 5,
    failSilently: Boolean = false
  ): T = {
    var attempt = 0
    while (attempt < maxRetries)
      Try(operation) match {
        case Success(result) => return result
        case Failure(e) if e.getMessage.contains("Code: 341") && attempt < maxRetries - 1 =>
          // Code 341: UNFINISHED - replication in progress, retry
          attempt += 1
          Thread.sleep(Math.pow(2, attempt).toLong * 1000)
        case Failure(e) if failSilently && attempt == maxRetries - 1 =>
          System.err.println(s"Warning: $operationName failed after $maxRetries attempts: ${e.getMessage}")
          return null.asInstanceOf[T]
        case Failure(e) =>
          throw new RuntimeException(s"$operationName failed after $attempt attempts", e)
      }
    throw new RuntimeException(s"$operationName exhausted $maxRetries retries")
  }
}
