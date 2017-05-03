organizationName := "com.github.nechaevv"

name := "postgresql-reactive"

scalaVersion := "2.12.2"

val akkaVersion = "2.5.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.typesafe.slick" %% "slick" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.2"
)
