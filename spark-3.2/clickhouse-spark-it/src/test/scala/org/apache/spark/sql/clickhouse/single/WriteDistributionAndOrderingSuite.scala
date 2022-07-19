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

import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, Row}

import java.sql.Date
import java.time.LocalDate

class WriteDistributionAndOrderingSuite extends SparkClickHouseSingleTest {

  import testImplicits._

  test("write repartitionByPartition") {
    autoCleanupTable("db_repartitionByPartition", "tbl_repartitionByPartition") { (db, tbl) =>
      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |    `id` String,
           |    `load_date` Date
           |) ENGINE = MergeTree
           |ORDER BY load_date
           |PARTITION BY xxHash64(id)
           |""".stripMargin
      )
      import org.apache.spark.sql.functions._

      withSQLConf(WRITE_REPARTITION_BY_PARTITION.key -> "true") {
        intercept[AnalysisException] {
          spark.range(3)
            .toDF("id")
            .withColumn("id", $"id".cast(StringType))
            .withColumn("load_date", lit(LocalDate.of(2022, 5, 27)))
            .writeTo(s"$db.$tbl")
            .append
        }
      }

      withSQLConf(WRITE_REPARTITION_BY_PARTITION.key -> "false") {
        spark.range(3)
          .toDF("id")
          .withColumn("id", $"id".cast(StringType))
          .withColumn("load_date", lit(LocalDate.of(2022, 5, 27)))
          .writeTo(s"$db.$tbl")
          .append

        checkAnswer(
          spark.sql(s"SELECT id, load_date FROM $db.$tbl"),
          Seq(
            Row("0", Date.valueOf("2022-05-27")),
            Row("1", Date.valueOf("2022-05-27")),
            Row("2", Date.valueOf("2022-05-27"))
          )
        )
        spark.sessionState.conf.unsetConf(WRITE_REPARTITION_BY_PARTITION)
      }
    }
  }

  test("write localSortByKey") {
    autoCleanupTable("db_localSortByKey", "tbl_localSortByKey") { (db, tbl) =>
      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |    `id` String,
           |    `load_date` Date
           |) ENGINE = MergeTree
           |ORDER BY xxHash64(id)
           |PARTITION BY load_date
           |""".stripMargin
      )

      withSQLConf(WRITE_LOCAL_SORT_BY_KEY.key -> "true") {
        intercept[AnalysisException] {
          spark.range(3)
            .toDF("id")
            .withColumn("id", $"id".cast(StringType))
            .withColumn("load_date", lit(LocalDate.of(2022, 5, 27)))
            .writeTo(s"$db.$tbl")
            .append
        }
      }

      withSQLConf(WRITE_LOCAL_SORT_BY_KEY.key -> "false") {
        spark.range(3)
          .toDF("id")
          .withColumn("id", $"id".cast(StringType))
          .withColumn("load_date", lit(LocalDate.of(2022, 5, 27)))
          .writeTo(s"$db.$tbl")
          .append

        checkAnswer(
          spark.sql(s"SELECT id, load_date FROM $db.$tbl"),
          Seq(
            Row("0", Date.valueOf("2022-05-27")),
            Row("1", Date.valueOf("2022-05-27")),
            Row("2", Date.valueOf("2022-05-27"))
          )
        )
      }
    }
  }
}
