name := "SampleProject"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"