ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.cryptostream"

name := "flink-job"

lazy val flinkVersion     = "1.20.0"
lazy val kafkaConnVersion = "3.3.0-1.20"

resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-streaming-java"          % flinkVersion,
  "org.apache.flink" % "flink-clients"                 % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka"         % kafkaConnVersion,
  "org.apache.flink" % "flink-avro-confluent-registry" % flinkVersion,
)

assembly / mainClass := Some("io.cryptostream.FlinkJob")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "module-info.class"      => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
