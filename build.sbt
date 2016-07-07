organizationName := "com.github.nechaevv"

name := "pg-akka-driver"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.1"

libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion
//      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
)
