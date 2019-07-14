name := "bigquery-class-gen"

version := "0.2.0"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.google.cloud"       % "google-cloud-bigquery" % "1.79.0",
  "org.scala-lang.modules" %% "scala-xml"            % "1.2.0",
  "org.scalameta"          %% "scalafmt-dynamic"     % "2.0.0",
  "org.scalatest"          %% "scalatest"            % "3.0.8" % "test"
)
