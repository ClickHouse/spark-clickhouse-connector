package xenon.clickhouse.spec

import java.util

import scala.collection.JavaConverters._

import xenon.clickhouse.ToJson

case class DatabaseSpec(
  name: String,
  engine: String,
  data_path: String,
  metadata_path: String,
  uuid: String
) extends ToJson {

  def toMap: Map[String, String] = Map(
    "name" -> name,
    "engine" -> engine,
    "data_path" -> data_path,
    "metadata_path" -> metadata_path,
    "uuid" -> uuid
  )

  def toJavaMap: util.Map[String, String] = toMap.asJava
}
