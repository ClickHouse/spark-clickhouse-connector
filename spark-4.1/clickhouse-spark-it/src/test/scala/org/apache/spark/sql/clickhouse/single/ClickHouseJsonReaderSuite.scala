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
import org.scalatest.tags.Cloud

@Cloud
class ClickHouseCloudJsonReaderSuite extends ClickHouseJsonReaderSuite with ClickHouseCloudMixIn

class ClickHouseSingleJsonReaderSuite extends ClickHouseJsonReaderSuite with ClickHouseSingleMixIn

/**
 * Test suite for ClickHouse JSON Reader.
 * Uses JSON format for reading data from ClickHouse (default in SparkClickHouseSingleTest).
 * All test cases are inherited from ClickHouseReaderTestBase.
 */
abstract class ClickHouseJsonReaderSuite extends ClickHouseReaderTestBase {
  // Uses JSON format (configured in SparkClickHouseSingleTest)
  // All tests are inherited from ClickHouseReaderTestBase
  // Additional JSON-specific tests can be added here if needed
}
