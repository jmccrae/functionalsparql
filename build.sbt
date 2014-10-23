name := "functionalsparql"

organization := "eu.liderproject"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.jena" % "jena-arq" % "2.12.1",
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.scalatest" %% "scalatest" % "2.1.7" % "test",
  "org.mockito" % "mockito-core" % "1.10.8" % "test"
)

