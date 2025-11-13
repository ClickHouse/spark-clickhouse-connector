package com.clickhouse.spark.spec

import com.clickhouse.spark.ToJson

case class MacrosSpec(
  name: String,
  substitution: String
) extends ToJson with Serializable {

  override def toString: String = s"macro: $name, substitution: $substitution"
}
