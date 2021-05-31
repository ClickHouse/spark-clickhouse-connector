package org.apache.spark.sql.clickhouse

import xenon.protocol.grpc.{Exception => GException}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.AnalysisException

case class ClickHouseAnalysisException(
  override val message: String,
  override val line: Option[Int] = None,
  override val startPosition: Option[Int] = None,
  // Some plans fail to serialize due to bugs in scala collections.
  @transient override val plan: Option[LogicalPlan] = None,
  override val cause: Option[Throwable] = None
) extends AnalysisException(message, line, startPosition, plan, cause) {

  def this(exception: GException) = this(message = s"Error[${exception.getCode}] ${exception.getDisplayText}")
}
