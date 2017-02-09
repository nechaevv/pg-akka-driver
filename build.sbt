organizationName := "com.github.nechaevv"

name := "postgresql-reactive"

scalaVersion := "2.12.1"

val akkaVersion = "2.4.16"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.typesafe.slick" %% "slick" % "3.2.0-M2",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)
