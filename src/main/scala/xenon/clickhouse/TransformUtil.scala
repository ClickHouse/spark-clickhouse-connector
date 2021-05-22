package xenon.clickhouse

import scala.util.matching.Regex

import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.ExpressionUtil._

object TransformUtil {
  // Some functions of ClickHouse which match Spark pre-defined Transforms
  //
  // toYear     - Converts a date or date with time to a UInt16 (AD). Alias: YEAR
  // toYYYYMM   - Converts a date or date with time to a UInt32 (YYYY * 100 + MM)
  // toYYYYMMDD - Converts a date or date with time to a UInt32 (YYYY * 10000 + MM * 100 + DD)
  // toHour     - Converts a date with time to a UInt8 (0-23). Alias: HOUR

  val years_transform_regex: Regex = """toYear\((.*)\)""".r
  val months_transform_regex: Regex = """toYYYYMM\((.*)\)""".r
  val days_transform_regex: Regex = """toYYYYMMDD\((.*)\)""".r
  val hours_transform_regex: Regex = """toHour\((.*)\)""".r

  def resolveTransform(transformExpr: String): Transform =
    transformExpr match {
      case years_transform_regex(expr) => years(reference(expr :: Nil))
      case months_transform_regex(expr) => months(reference(expr :: Nil))
      case days_transform_regex(expr) => days(reference(expr :: Nil))
      case hours_transform_regex(expr) => hours(reference(expr :: Nil))
      // Assume all others is just a column name without any transforms,
      // thus, `xxHash64(abc)` is not supported yet.
      case column_name => identity(reference(column_name :: Nil))
    }
}
