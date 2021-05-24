package xenon.clickhouse

import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.util.Using

class ClickHouseCommandRunner extends ExternalCommandRunner with ClickHouseHelper {

  override def executeCommand(sql: String, options: CaseInsensitiveStringMap): Array[String] =
    Using.resource(GrpcNodeClient(buildNodeSpec(options))) { grpcNodeClient =>
      Array(grpcNodeClient.syncQueryAndCheck(sql).getOutput)
    }
}
