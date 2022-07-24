/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.format

import java.io.InputStream

import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import xenon.clickhouse.JsonProtocol.om
import xenon.clickhouse.exception.ClickHouseClientException

///////////////////////////////////////////////////////////////////////////////
/////////////////////////////////// Simple ////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

object JSONEachRowSimpleOutput {
  def deserialize(input: InputStream): SimpleOutput[ObjectNode] = {
    val records = om.readerFor[ObjectNode]
      .readValues(input)
      .asScala.toSeq
    new JSONEachRowSimpleOutput(records)
  }
}

class JSONEachRowSimpleOutput(
  override val records: Seq[ObjectNode]
) extends SimpleOutput[ObjectNode]

object JSONCompactEachRowWithNamesAndTypesSimpleOutput {
  def deserialize(input: InputStream): SimpleOutput[Array[JsonNode]] with NamesAndTypes = {
    val jsonParser = om.getFactory.createParser(input)
    val records = om.readValues[Array[JsonNode]](jsonParser).asScala.toSeq
    new JSONCompactEachRowWithNamesAndTypesSimpleOutput(records)
  }
}

class JSONCompactEachRowWithNamesAndTypesSimpleOutput(
  //     first row : names
  //    second row : types
  // following rows: records
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

object JSONCompactEachRowWithNamesAndTypesStreamOutput {
  def deserializeStream(inputIterator: Iterator[InputStream]): StreamOutput[Array[JsonNode]] = {
    val stream = inputIterator.flatMap { output =>
      val jsonParser = om.getFactory.createParser(output)
      om.readValues[Array[JsonNode]](jsonParser).asScala
    }
    new JSONCompactEachRowWithNamesAndTypesStreamOutput(stream)
  }
}

class JSONCompactEachRowWithNamesAndTypesStreamOutput(
  //     first row : names
  //    second row : types
  // following rows: records
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
