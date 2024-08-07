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

name := "ClickHouse Apache Spark 3.5 Example Project"

version := "1.0"

scalaVersion := "2.12.18"

resolvers += "Maven Repo" at  "https://s01.oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

libraryDependencies += "com.clickhouse" % "clickhouse-jdbc" % "0.6.3" classifier "all"
libraryDependencies += "com.clickhouse.spark" %% "clickhouse-spark-runtime-3.5" % "0.8.0-SNAPSHOT"

