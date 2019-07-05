name := "bigquery-case-class-gen"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.typesafe"           % "config"                % "1.3.4",
  "com.google.cloud"       % "google-cloud-bigquery" % "1.79.0",
  "org.scala-lang.modules" %% "scala-xml"            % "1.2.0",
  "org.scalameta"          %% "scalafmt-dynamic"     % "2.0.0"
)
