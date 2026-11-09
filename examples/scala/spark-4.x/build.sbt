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

name := "clickhouse-spark-examples"

version := "1.0"

// Pick the Spark line with `sbt -Dspark_binary_version=4.1 ...` (default 4.0).
// Spark versions match the ones the connector builds against (gradle.properties). Scala must be at least
// the scala-library on the dependency classpath, or sbt refuses to compile: Spark 4.1.4 pulls in 2.13.18
// through jackson-module-scala.
val sparkBinaryVersion = sys.props.getOrElse("spark_binary_version", "4.0")

// spark_binary_version -> (Spark version, Scala version)
val sparkVersions = Map(
  "4.0" -> ("4.0.1", "2.13.16"),
  "4.1" -> ("4.1.4", "2.13.18"),
  "4.2" -> ("4.2.1", "2.13.18")
)

val selectedVersions = sparkVersions.getOrElse(
  sparkBinaryVersion,
  sys.error(
    s"Unsupported spark_binary_version '$sparkBinaryVersion'; expected one of ${sparkVersions.keys.toSeq.sorted.mkString(", ")}"
  )
)

val sparkVersion = selectedVersions._1

// Override with `-Dclickhouse_connector_version=...` (the 4.1 / 4.2 runtimes are only
// published from the first release that includes them).
val clickhouseConnectorVersion = sys.props.getOrElse("clickhouse_connector_version", "0.10.2")

scalaVersion := selectedVersions._2

// Note: For production Spark applications, add % "provided" to Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.clickhouse.spark" %% s"clickhouse-spark-runtime-$sparkBinaryVersion" % clickhouseConnectorVersion
)

// For local debugging: uncomment to use locally built connector JAR instead of Maven
// This allows you to debug connector code changes without publishing to Maven
// (build it first with `./gradlew :clickhouse-spark-runtime-<v>_2.13:shadowJar -Dspark_binary_version=<v>`)
// Compile / unmanagedJars += {
//   val runtimeJar =
//     baseDirectory.value / ".." / ".." / ".." / s"spark-$sparkBinaryVersion" / "clickhouse-spark-runtime" / "build" / "libs" /
//       s"clickhouse-spark-runtime-${sparkBinaryVersion}_2.13-$clickhouseConnectorVersion.jar"
//   Attributed.blank(runtimeJar)
// }

// Fork the JVM for run to avoid classpath issues
fork := true

// Add JVM options for Arrow
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
