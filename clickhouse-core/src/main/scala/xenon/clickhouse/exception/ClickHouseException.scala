package xenon.clickhouse.exception

import xenon.protocol.grpc.{Exception => GRPCException}

trait ClickHouseException extends RuntimeException {
  def code: Int
  def reason: String
}

case class ClickHouseServerException(
  override val code: Int,
  override val reason: String
) extends ClickHouseException {
  def this(exception: GRPCException) = this(exception.getCode, exception.getDisplayText)
}

case class ClickHouseClientException(
  override val reason: String
) extends ClickHouseException {
  def code: Int = ClickHouseErrCode.CLIENT_ERROR.code()
}

case class RetryableClickHouseException(
  override val code: Int,
  override val reason: String
) extends ClickHouseException {
  def this(exception: GRPCException) = this(exception.getCode, exception.getDisplayText)
}
