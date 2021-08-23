package org.apache.spark.sql.clickhouse

import org.apache.spark._
import org.apache.spark.util.VersionUtils

object SparkUtils {

  lazy val MAJOR_MINOR_VERSION: (Int, Int) = VersionUtils.majorMinorVersion(SPARK_VERSION)
}
