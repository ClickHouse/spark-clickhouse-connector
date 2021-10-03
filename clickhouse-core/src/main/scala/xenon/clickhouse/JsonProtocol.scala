package xenon.clickhouse

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.ClassTagExtensions

trait ToJson {

  def toJson: String = JsonProtocol.toJson(this)

  override def toString: String = toJson
}

trait FromJson[T] {
  def fromJson(json: String): T
}

trait JsonProtocol[T] extends FromJson[T] with ToJson

object JsonProtocol {

  @transient lazy val om: ObjectMapper with ClassTagExtensions = {
    val _om = new ObjectMapper() with ClassTagExtensions
    _om.findAndRegisterModules()
    _om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _om
  }

  def toJson(value: Any): String = om.writeValueAsString(value)

  def toPrettyJson(value: Any): String = om.writer(SerializationFeature.INDENT_OUTPUT).writeValueAsString(value)
}
