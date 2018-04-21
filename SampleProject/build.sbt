name := "SampleProject"

version := "1.0"

scalaVersion := "2.11.8"

fork := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.12.1" % "provided"
)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0" % "provided"