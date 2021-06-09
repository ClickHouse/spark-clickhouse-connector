package xenon.clickhouse

import scala.util.Using

import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import xenon.clickhouse.grpc.GrpcNodeClient

class ClickHouseCommandRunner extends ExternalCommandRunner with ClickHouseHelper {

  override def executeCommand(sql: String, options: CaseInsensitiveStringMap): Array[String] =
    Using.resource(GrpcNodeClient(buildNodeSpec(options))) { grpcNodeClient =>
      // TODO refactor the output as tableau
      Array(grpcNodeClient.syncQueryAndCheck(sql).getOutput)
    }
}
