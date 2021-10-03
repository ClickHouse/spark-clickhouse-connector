package xenon.clickhouse.spec

import java.time.LocalDateTime
import java.util

import scala.collection.JavaConverters._

import xenon.clickhouse.ToJson

case class TableSpec(
  database: String,
  name: String,
  uuid: String,
  engine: String,
  is_temporary: Boolean,
  data_paths: Array[String],
  metadata_path: String,
  metadata_modification_time: LocalDateTime,
  dependencies_database: Array[String],
  dependencies_table: Array[String],
  create_table_query: String,
  engine_full: String,
  partition_key: String,
  sorting_key: String,
  primary_key: String,
  sampling_key: String,
  storage_policy: String,
  total_rows: Option[Long],
  total_bytes: Option[Long],
  lifetime_rows: Option[Long],
  lifetime_bytes: Option[Long]
) extends ToJson {
  def toMap: Map[String, String] = Map(
    "database" -> database,
    "name" -> name,
    "uuid" -> uuid,
    "engine" -> engine,
    "is_temporary" -> is_temporary.toString,
    "data_paths" -> data_paths.mkString(","),
    "metadata_path" -> metadata_path,
    "metadata_modification_time" -> metadata_modification_time.toString,
    "dependencies_database" -> dependencies_database.mkString(","),
    "dependencies_table" -> dependencies_table.mkString(","),
    "create_table_query" -> create_table_query,
    "engine_full" -> engine_full,
    "partition_key" -> partition_key,
    "sorting_key" -> sorting_key,
    "primary_key" -> primary_key,
    "sampling_key" -> sampling_key,
    "storage_policy" -> storage_policy,
    "total_rows" -> total_rows.map(_.toString).getOrElse(""),
    "total_bytes" -> total_bytes.map(_.toString).getOrElse(""),
    "lifetime_rows" -> lifetime_rows.map(_.toString).getOrElse(""),
    "lifetime_bytes" -> lifetime_bytes.map(_.toString).getOrElse("")
  )

  def toJavaMap: util.Map[String, String] = toMap.asJava
}
