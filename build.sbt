
version := "1.0"

scalaVersion := "2.10.6"

lazy val root = (project in file("."))
.settings(
  name := "sparksql"
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.10.6",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-sql" % "1.6.2",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "commons-cli" % "commons-cli" % "1.4",
  "org.apache.hadoop" % "hadoop-common" % "2.7.4"
    exclude("javax.servlet", "servlet-api")
)
