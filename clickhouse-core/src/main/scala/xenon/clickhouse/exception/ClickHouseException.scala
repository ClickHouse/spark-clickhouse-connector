package xenon.clickhouse.exception

import xenon.protocol.grpc.{Exception => GRPCException}

abstract class ClickHouseException(code: Int, reason: String) extends RuntimeException(s"[$code] $reason")

case class ClickHouseServerException(code: Int, reason: String) extends ClickHouseException(code, reason) {
  def this(exception: GRPCException) = this(exception.getCode, exception.getDisplayText)
}

case class ClickHouseClientException(reason: String)
    extends ClickHouseException(ClickHouseErrCode.CLIENT_ERROR.code(), reason)

case class RetryableClickHouseException(code: Int, reason: String) extends ClickHouseException(code, reason) {
  def this(exception: GRPCException) = this(exception.getCode, exception.getDisplayText)
}
