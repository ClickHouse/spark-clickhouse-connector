package xenon.clickhouse.exception

import xenon.protocol.grpc.{Exception => GException}

trait ClickHouseException extends RuntimeException {
  def code: Int
  def msg: String
}

case class ClickHouseServerException(
  override val code: Int,
  override val msg: String
) extends ClickHouseException {
  def this(exception: GException) = this(exception.getCode, exception.getDisplayText)
}

case class ClickHouseClientException(
  override val msg: String
) extends ClickHouseException {
  def code: Int = ClickHouseErrCode.CLIENT_ERROR.code()
}

case class RetryableClickHouseException(
  override val code: Int,
  override val msg: String
) extends ClickHouseException {
  def this(exception: GException) = this(exception.getCode, exception.getDisplayText)
}
