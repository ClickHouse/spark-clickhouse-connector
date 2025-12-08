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

scalaVersion := "2.13.8"

val sparkVersion = "4.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)

// For local development, include the connector from the parent project
unmanagedClasspath in Compile ++= {
  val base = baseDirectory.value / ".." / "clickhouse-spark" / "build" / "classes"
  Seq(
    Attributed.blank(base / "scala" / "main"),
    Attributed.blank(base / "java" / "main")
  )
}
