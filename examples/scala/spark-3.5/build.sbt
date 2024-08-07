name := "ClickHouse Apache Spark 3.5 Example Project"

version := "1.0"

scalaVersion := "2.12.18"

resolvers += "Maven Repo" at  "https://s01.oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

libraryDependencies += "com.clickhouse" % "clickhouse-jdbc" % "0.6.3" classifier "all"
//libraryDependencies += "com.clickhouse" % "clickhouse-client" % "0.6.3"
//libraryDependencies += "com.clickhouse" % "clickhouse-http-client" % "0.6.3"
//libraryDependencies += "com.clickhouse" % "clickhouse-data" % "0.6.3"

libraryDependencies += "com.clickhouse.spark" %% "clickhouse-spark-runtime-3.5" % "0.8.0-SNAPSHOT"

