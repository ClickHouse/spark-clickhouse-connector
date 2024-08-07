
import org.apache.spark.sql.SparkSession

object SparkTestApp {
  def main(args: Array[String]): Unit = {
    val username = "default"
    val password = "replace with password"
    val host = "replace with host"

    val spark = SparkSession.builder.appName("ClickHouse Apache Spark 3.5 Example")
      .master("local[2]")
      .config("spark.sql.catalog.clickhouse","com.clickhouse.spark.ClickHouseCatalog")
      .config("spark.sql.catalog.clickhouse.host", host)
      .config("spark.sql.catalog.clickhouse.protocol","http")
      .config("spark.sql.catalog.clickhouse.http_port","8443")
      .config("spark.sql.catalog.clickhouse.user", username)
      .config("spark.sql.catalog.clickhouse.password", password)
      .config("spark.sql.catalog.clickhouse.database","default")
      .config("spark.sql.catalog.clickhouse.option.ssl","true")
      .getOrCreate()

    spark.sql("use clickhouse")
    spark.sql("show tables").show()
    spark.stop()
  }
}
