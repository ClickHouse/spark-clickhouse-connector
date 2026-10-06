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

import java.time.Instant

@Cloud
class ClickHouseCloudBinaryReaderSuite extends ClickHouseBinaryReaderSuite with ClickHouseCloudMixIn

class ClickHouseSingleBinaryReaderSuite extends ClickHouseBinaryReaderSuite with ClickHouseSingleMixIn

/**
 * Test suite for ClickHouse Binary Reader.
 * Uses binary format for reading data from ClickHouse.
 * All test cases are inherited from ClickHouseReaderTestBase.
 */
abstract class ClickHouseBinaryReaderSuite extends ClickHouseReaderTestBase {

  // Override to use binary format for reading
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.read.format", "binary")
    .set("spark.clickhouse.write.format", "arrow")

  // All tests are inherited from ClickHouseReaderTestBase
  // Additional binary-specific tests can be added here if needed

  test("decode TimestampType - DateTime64 with column timezone, exact instants") {
    withKVTable("test_db", "test_datetime64_tz", valueColDef = "DateTime64(3, 'Asia/Kolkata')") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.$actualTbl VALUES
             |(1, toDateTime64('2024-06-15 12:30:45.123', 3, 'UTC')),
             |(2, toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'))
             |""".stripMargin
        )

        val df = readTableWithBothAPIs(actualDb, actualTbl, columns = "key, value", orderBy = Some("key"))
        assert(df.collect().map(_.getTimestamp(1).toInstant).toSeq == Seq(
          Instant.parse("2024-06-15T12:30:45.123Z"),
          Instant.parse("1969-12-31T23:59:59.999Z")
        ))
    }
  }

  test("decode DecimalType - Decimal(18, 4) exact values") {
    withKVTable("test_db", "test_decimal18_exact", valueColDef = "Decimal(18, 4)") {
      (actualDb: String, actualTbl: String) =>
        runClickHouseSQL(
          s"""INSERT INTO $actualDb.$actualTbl VALUES
             |(1, toDecimal64('99999999999999.9999', 4)),
             |(2, toDecimal64('-99999999999999.9999', 4)),
             |(3, toDecimal64('0', 4)),
             |(4, toDecimal64('1.5', 4))
             |""".stripMargin
        )

        val df = readTableWithBothAPIs(actualDb, actualTbl, columns = "key, value", orderBy = Some("key"))
        assert(df.collect().map(_.getDecimal(1)).toSeq ==
          Seq("99999999999999.9999", "-99999999999999.9999", "0.0000", "1.5000").map(new java.math.BigDecimal(_)))
    }
  }
}
