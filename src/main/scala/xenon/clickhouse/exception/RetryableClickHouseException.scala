package xenon.clickhouse.exception

import com.zy.dp.xenon.protocol.grpc.{Exception => GException}

case class RetryableClickHouseException(code: Int, msg: String) extends ClickHouseException(code, msg) {
  def this(exception: GException) = this(exception.getCode, exception.getDisplayText)
}
