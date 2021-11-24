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

  def withTable(
    db: String,
    tbl: String,
    cols: Seq[StructField],
    engine: String = "MergeTree()",
    sortKey: String = "id",
    partKey: Option[String] = None
  )(f: => Unit): Unit =
    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db")

      spark.sql(
        s"""CREATE TABLE $db.$tbl (
           |  ${cols.map(_.toDDL).mkString(",\n  ")}
           |) USING ClickHouse
           |${partKey.map(k => s"PARTITIONED BY ($k)").getOrElse("")}
           |TBLPROPERTIES (
           |  engine = '$engine',
           |  order_by = '($sortKey)'
           |)
           |""".stripMargin
      )

      f
    } finally {
      runClickHouseSQL(s"DROP TABLE IF EXISTS $db.$tbl")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS $db")
    }

  def withSimpleTable(
    db: String,
    tbl: String,
    writeData: Boolean = false
  )(f: => Unit): Unit =
    try {
      runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS $db")

      // SPARK-33779: Spark 3.2 only support IdentityTransform
      spark.sql(
        s"""CREATE TABLE $db.$tbl (
           |  id          BIGINT    NOT NULL COMMENT 'sort key',
           |  value       STRING,
           |  create_time TIMESTAMP NOT NULL,
           |  m           INT       NOT NULL COMMENT 'part key'
           |) USING ClickHouse
           |PARTITIONED BY (m)
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = '(id)'
           |)
           |""".stripMargin
      )

      if (writeData) {
        val tblSchema = spark.table(s"$db.$tbl").schema
        val dataDF = spark.createDataFrame(Seq(
          (1L, "1", "2021-01-01 10:10:10"),
          (2L, "2", "2022-02-02 10:10:10")
        )).toDF("id", "value", "create_time")
          .withColumn("create_time", to_timestamp($"create_time"))
          .withColumn("m", month($"create_time"))
          .select($"id", $"value", $"create_time", $"m")

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
