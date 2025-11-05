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

package org.apache.spark.sql.clickhouse.single

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn}
import org.apache.spark.SparkConf
import org.scalatest.tags.Cloud

@Cloud
class ClickHouseCloudBinaryReaderSuite extends ClickHouseBinaryReaderSuite with ClickHouseCloudMixIn

class ClickHouseSingleBinaryReaderSuite extends ClickHouseBinaryReaderSuite with ClickHouseSingleMixIn

/**
 * Test suite for ClickHouse Binary Reader.
 * Uses binary format for reading data from ClickHouse.
 * All test cases are inherited from ClickHouseReaderTestBase.
 */
abstract class ClickHouseBinaryReaderSuite extends ClickHouseReaderTestBase {
  
  // Override to use binary format instead of JSON
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.read.format", "binary")
    .set("spark.clickhouse.write.format", "binary")

  // All tests are inherited from ClickHouseReaderTestBase
  // Additional binary-specific tests can be added here if needed
}
