organizationName := "com.github.nechaevv"

name := "pg-akka-driver"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.8"

libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion
)
