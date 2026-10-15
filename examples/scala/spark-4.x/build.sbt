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

scalaVersion := "2.13.16"

val sparkVersion = "4.0.1"
val clickhouseConnectorVersion = "0.9.0"

// Note: For production Spark applications, add % "provided" to Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.clickhouse.spark" %% "clickhouse-spark-runtime-4.0" % clickhouseConnectorVersion
)

// For local debugging: uncomment to use locally built connector JAR instead of Maven
// This allows you to debug connector code changes without publishing to Maven
// unmanagedJars in Compile += {
//   val runtimeJar =
//     baseDirectory.value / ".." / ".." / ".." / "spark-4.0" / "clickhouse-spark-runtime" / "build" / "libs" / "clickhouse-spark-runtime-4.0_2.13-0.9.0-SNAPSHOT.jar"
//   Attributed.blank(runtimeJar)
// }

// Fork the JVM for run to avoid classpath issues
fork := true

// Add JVM options for Arrow
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
