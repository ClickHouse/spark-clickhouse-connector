package xenon.clickhouse.format

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import xenon.clickhouse.exception.ClickHouseClientException

import scala.collection.immutable.ListMap

///////////////////////////////////////////////////////////////////////////////
/////////////////////////////////// Simple ////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class JSONEachRowSimpleOutput(
  override val records: Seq[ObjectNode]
) extends SimpleOutput[ObjectNode]

class JSONCompactEachRowWithNamesAndTypesSimpleOutput(
  //     first row: names
  //    second row: types
  // following row: records
  namesAndTypesData: Seq[Array[JsonNode]]
) extends SimpleOutput[Array[JsonNode]] with NamesAndTypes {

  private val _namesAndTypes: ListMap[String, String] = namesAndTypesData.take(2).toList match {
    case names :: types :: Nil => ListMap.empty ++ (names.map(_.asText) zip types.map(_.asText))
    case _ => throw ClickHouseClientException("Corrupt data of output format JSONCompactEachRowWithNamesAndTypes")
  }

  private val _records: Seq[Array[JsonNode]] = namesAndTypesData.drop(2)

  override def namesAndTypes: ListMap[String, String] = _namesAndTypes

  override def records: Seq[Array[JsonNode]] = _records
}

///////////////////////////////////////////////////////////////////////////////
/////////////////////////////////// Stream ////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class JSONCompactEachRowWithNamesAndTypesStreamOutput(
  //     first row: names
  //    second row: types
  // following row: records
  namesAndTypesDataStream: Iterator[Array[JsonNode]]
) extends StreamOutput[Array[JsonNode]] with NamesAndTypes {

  private var names: Array[String] = _
  private var types: Array[String] = _

  override lazy val namesAndTypes: ListMap[String, String] = {
    hasNext
    require(names != null && types != null)
    ListMap.empty ++ (names zip types)
  }

  private var nextRecord: Array[JsonNode] = _

  override def hasNext: Boolean = {
    if (nextRecord != null)
      return true

    while (namesAndTypesDataStream.hasNext)
      if (names == null)
        names = namesAndTypesDataStream.next.map(_.asText)
      else if (types == null)
        types = namesAndTypesDataStream.next.map(_.asText)
      else {
        nextRecord = namesAndTypesDataStream.next
        return true
      }
    false
  }

  override def next: Array[JsonNode] = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    val ret = nextRecord
    nextRecord = null
    ret
  }
}
