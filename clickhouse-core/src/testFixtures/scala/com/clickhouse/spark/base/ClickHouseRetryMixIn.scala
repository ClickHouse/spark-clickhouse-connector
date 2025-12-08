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

/**
 * Database and table lifecycle operations with retry logic for ClickHouse replication.
 * These methods accept a SQL executor function as a parameter to avoid type dependencies.
 */
object ClickHouseRetryMixIn {

  def createDatabaseWithRetry(
    db: String,
    sqlExecutor: String => Any,
    maxRetries: Int = 5
  ): Unit =
    RetryUtils.retryOnReplicationError(
      sqlExecutor(s"CREATE DATABASE IF NOT EXISTS `$db`"),
      s"Create database $db",
      maxRetries
    )

  def dropDatabaseWithRetry(
    db: String,
    sqlExecutor: String => Any,
    maxRetries: Int = 5
  ): Unit =
    RetryUtils.retryOnReplicationError(
      sqlExecutor(s"DROP DATABASE IF EXISTS `$db`"),
      s"Drop database $db",
      maxRetries,
      failSilently = true
    )

  def dropTableWithRetry(
    db: String,
    tbl: String,
    sqlExecutor: String => Any,
    maxRetries: Int = 5
  ): Unit =
    RetryUtils.retryOnReplicationError(
      sqlExecutor(s"DROP TABLE IF EXISTS `$db`.`$tbl`"),
      s"Drop table $db.$tbl",
      maxRetries,
      failSilently = true
    )
}
