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

package org.apache.spark.sql.clickhouse

import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.SEND_ANONYMOUS_USAGE_STATS
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class SparkOptionsSuite extends AnyFunSuite {

  private def readOptions(options: Map[String, String] = Map.empty): ReadOptions =
    new ReadOptions(options.asJava)

  private def writeOptions(options: Map[String, String] = Map.empty): WriteOptions =
    new WriteOptions(options.asJava)

  test("sendAnonymousUsageStats defaults to true") {
    assert(readOptions().sendAnonymousUsageStats)
    assert(writeOptions().sendAnonymousUsageStats)
  }

  test("sendAnonymousUsageStats can be disabled per job through the options map") {
    val disabled = Map(SEND_ANONYMOUS_USAGE_STATS.key -> "false")
    assert(!readOptions(disabled).sendAnonymousUsageStats)
    assert(!writeOptions(disabled).sendAnonymousUsageStats)
  }

  test("sendAnonymousUsageStats falls back to the session conf") {
    SQLConf.get.setConfString(SEND_ANONYMOUS_USAGE_STATS.key, "false")
    try {
      assert(!readOptions().sendAnonymousUsageStats)
      assert(!writeOptions().sendAnonymousUsageStats)
    } finally SQLConf.get.unsetConf(SEND_ANONYMOUS_USAGE_STATS.key)
  }
}
