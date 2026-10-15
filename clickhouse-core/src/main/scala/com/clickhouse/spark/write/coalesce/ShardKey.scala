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

package com.clickhouse.spark.write.coalesce

/**
 * Identifies a coalescing target on one executor: a (write job, database, table, shard) tuple.
 * `writeId` (the Spark write's queryId) scopes buckets to a single write job, so two writes to the
 * same table name (e.g. after a drop/recreate with a different schema) never share a bucket or
 * its BucketContext.
 */
final case class ShardKey(writeId: String, database: String, table: String, shardNum: Option[Int])
