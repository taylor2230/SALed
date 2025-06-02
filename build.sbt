ThisBuild / version := "0.0.1"

ThisBuild / scalaVersion := "3.4.2"

val arrowVersion: String = "18.2.0"
val icebergVersion: String = "1.6.1"

val baseLibraries: Seq[ModuleID] = Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
  "org.json4s" %% "json4s-native" % "4.0.7",
  "org.json4s" %% "json4s-jackson" % "4.0.7"
)

val arrowLibraries: Seq[ModuleID] = Seq(
  "org.apache.arrow" % "arrow-vector" % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty" % arrowVersion % "runtime"

)

val icebergLibraries: Seq[ModuleID] = Seq(
  "org.apache.iceberg" % "iceberg-core" % icebergVersion % "runtime",
  "org.apache.iceberg" % "iceberg-common" % icebergVersion % "runtime",
  "org.apache.iceberg" % "iceberg-api" % icebergVersion,
  "org.apache.iceberg" % "iceberg-data" % icebergVersion

)

val testingLibraries: Seq[ModuleID] = Seq(
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,

)

lazy val root = (project in file("."))
  .settings(
    name := "SALed",
    idePackagePrefix := Some("org.saled"),
    libraryDependencies ++= baseLibraries ++ arrowLibraries ++ icebergLibraries ++ testingLibraries
  )
