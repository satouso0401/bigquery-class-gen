name := "bigquery-case-class-gen"

version := "0.1"

scalaVersion := "2.13.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.4",
  "com.google.cloud" % "google-cloud-bigquery" % "1.79.0"
)

