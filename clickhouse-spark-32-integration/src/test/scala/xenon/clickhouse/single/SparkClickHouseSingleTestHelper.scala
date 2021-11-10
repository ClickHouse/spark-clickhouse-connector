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

package xenon.clickhouse.single

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import xenon.clickhouse.BaseSparkSuite

trait SparkClickHouseSingleTestHelper { self: BaseSparkSuite with SparkClickHouseSingleMixin =>
  import spark.implicits._

  def withTable(db: String, tbl: String, writeData: Boolean = false)(f: => Unit): Unit = {
    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db")

      // SPARK-33779: Spark 3.2 only support IdentityTransform
      spark.sql(
        s"""CREATE TABLE $db.$tbl (
           |  create_time TIMESTAMP NOT NULL,
           |  m           INT       NOT NULL COMMENT 'part key',
           |  id          BIGINT    NOT NULL COMMENT 'sort key',
           |  value       STRING
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = '(id)',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )

      if (writeData) {
        val tblSchema = spark.table(s"$db.$tbl").schema
        val dataDF = spark.createDataFrame(Seq(
          ("2021-01-01 10:10:10", 1L, "1"),
          ("2022-02-02 10:10:10", 2L, "2")
        )).toDF("create_time", "id", "value")
          .withColumn("create_time", to_timestamp($"create_time"))
          .withColumn("m", month($"create_time"))
          .select($"create_time", $"m", $"id", $"value")

        spark.createDataFrame(dataDF.rdd, tblSchema)
          .writeTo(s"$db.$tbl")
          .append
      }

      f
    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db")
    }
  }
}
