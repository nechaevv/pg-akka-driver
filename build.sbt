organizationName := "com.github.nechaevv"

name := "pg-akka-driver"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
)
