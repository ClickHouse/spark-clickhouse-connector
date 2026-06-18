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

package org.apache.spark.sql.clickhouse

import com.clickhouse.spark.Utils
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
 * End-to-end test cases for configuration documentation.
 *
 * The golden result file is "docs/configurations/02_sql_configurations.md".
 *
 * To run the entire test suite:
 * {{{
 *   ./gradlew test --tests=ConfigurationSuite
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   UPDATE=1 ./gradlew test --tests=ConfigurationSuite
 * }}}
 */
class ConfigurationSuite extends AnyFunSuite {

  private val configurationsMarkdown = Paths
    .get(Utils.getCodeSourceLocation(getClass).split("clickhouse-spark").head)
    .resolve("..")
    .resolve("docs")
    .resolve("configurations")
    .resolve("02_sql_configurations.md")
    .normalize

  test("docs") {
    ClickHouseSQLConf

    val newOutput = new ArrayBuffer[String]
    newOutput += "---"
    newOutput += "license: |"
    newOutput += "  Licensed under the Apache License, Version 2.0 (the \"License\");"
    newOutput += "  you may not use this file except in compliance with the License."
    newOutput += "  You may obtain a copy of the License at"
    newOutput += "  "
    newOutput += "      https://www.apache.org/licenses/LICENSE-2.0"
    newOutput += "  "
    newOutput += "  Unless required by applicable law or agreed to in writing, software"
    newOutput += "  distributed under the License is distributed on an \"AS IS\" BASIS,"
    newOutput += "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied."
    newOutput += "  See the License for the specific language governing permissions and"
    newOutput += "  limitations under the License."
    newOutput += "---"
    newOutput += ""
    newOutput += "<!--begin-include-->"
    newOutput += "|Key | Default | Description | Since"
    newOutput += "|--- | ------- | ----------- | -----"

    val sqlConfEntries: Seq[ConfigEntry[_]] =
      ru.runtimeMirror(SQLConf.getClass.getClassLoader)
        .reflect(SQLConf)
        .reflectField(ru.typeOf[SQLConf.type].decl(ru.TermName("sqlConfEntries")).asTerm)
        .get.asInstanceOf[util.Map[String, ConfigEntry[_]]]
        .asScala.values.toSeq

    sqlConfEntries
      .filter(entry => entry.key.startsWith("spark.clickhouse.") && entry.isPublic)
      .sortBy(_.key)
      .foreach { entry =>
        val seq = Seq(
          s"${entry.key}",
          s"${entry.defaultValueString}",
          s"${entry.doc}",
          s"${entry.version}"
        )
        newOutput += seq.mkString("|")
      }
    newOutput += "<!--end-include-->"

    verifyOutput(configurationsMarkdown, newOutput, getClass.getCanonicalName)
  }

  test("write server settings") {
    val conf = SQLConf.get
    val prefix = ClickHouseSQLConf.WRITE_SERVER_SETTINGS_PREFIX
    val previousWriteSettings = conf.getAllConfs
      .filter { case (key, _) => key.startsWith(prefix) }
    conf.getAllConfs.keys.filter(_.startsWith(prefix)).toSeq.foreach(conf.unsetConf)
    conf.setConfString(s"${prefix}log_comment", "MiXeD")
    conf.setConfString(s"${prefix}insert_deduplication_token", "FromConf")
    conf.setConfString(s"${prefix}custom_encoded", "a,b=[One,Two]")
    try {
      val writerOptions = new util.HashMap[String, String]()
      writerOptions.put(s"${prefix}wait_for_async_insert", "1")
      writerOptions.put(s"${prefix}insert_deduplication_token", "FromWriteOption")

      val settings = new WriteOptions(writerOptions).settings
      assert(settings("log_comment") === "MiXeD")
      assert(settings("custom_encoded") === "a,b=[One,Two]")
      assert(settings("wait_for_async_insert") === "1")
      assert(settings("insert_deduplication_token") === "FromWriteOption")

      val malformedOptions = new util.HashMap[String, String]()
      malformedOptions.put(prefix, "async_insert")
      intercept[IllegalArgumentException](new WriteOptions(malformedOptions).settings)
    } finally {
      conf.getAllConfs.keys.filter(_.startsWith(prefix)).toSeq.foreach(conf.unsetConf)
      previousWriteSettings.foreach { case (key, value) => conf.setConfString(key, value) }
    }
  }

  def verifyOutput(goldenFile: Path, newOutput: ArrayBuffer[String], agent: String): Unit =
    if (System.getenv("UPDATE") == "1") {
      val writer = Files.newBufferedWriter(
        goldenFile,
        StandardCharsets.UTF_8,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.CREATE
      )
      try newOutput.foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      finally writer.close()
    } else {
      val expected = Files.readAllLines(goldenFile).asScala
        .filterNot(line => line.contains("[Spark 4.0+ only]"))
      val hint = s"$goldenFile is out of date, please update the golden file with " +
        s"UPDATE=1 ./gradlew test --tests=ConfigurationSuite"
      assert(newOutput.size === expected.size, hint)

      newOutput.zip(expected).foreach { case (out, in) => assert(out === in, hint) }
    }
}
