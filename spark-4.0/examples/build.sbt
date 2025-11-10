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
