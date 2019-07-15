organization := "com.github.satouso0401"
name := "bigquery-class-gen"

version := "0.2.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.google.cloud"       % "google-cloud-bigquery" % "1.79.0",
  "org.scala-lang.modules" %% "scala-xml"            % "1.2.0",
  "org.scalameta"          %% "scalafmt-dynamic"     % "2.0.0",
  "org.scalatest"          %% "scalatest"            % "3.0.8" % "test"
)

licenses := Seq("MIT" -> url("https://github.com/satouso0401/bigquery-class-gen/blob/master/LICENSE"))
homepage := Some(url("https://github.com/satouso0401/bigquery-class-gen"))

publishMavenStyle := true
publishArtifact in Test := false
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/satouso0401/bigquery-class-gen"),
    "scm:git@github.com:satouso0401/bigquery-class-gen.git"
  )
)

developers := List(
  Developer(
    id    = "satouso0401",
    name  = "Takumi Sato",
    email = "satouso0401@gmail.com",
    url   = url("https://github.com/satouso0401/bigquery-class-gen")
  )
)
