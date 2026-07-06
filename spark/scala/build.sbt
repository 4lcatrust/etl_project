ThisBuild / scalaVersion := "2.13.18"
val sparkV = "4.0.3" // match the deso-query cluster (apache/spark:4.0.3-scala2.13-...)

name := "bronze-extractor"
version := "0.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkV % "provided",
  "org.json4s"       %% "json4s-native" % "4.0.7"
)

assembly / mainClass := Some("jobs.BronzeExtract")
assembly / assemblyJarName := s"bronze-extractor-assembly-${version.value}.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}
