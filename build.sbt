import Dependencies._
import sbt.Keys.libraryDependencies

val sparkVersion = "2.2.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "KafkaAgents",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion, // % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion, // % "provided",
    libraryDependencies +=  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion // % "provided"
//    libraryDependencies += "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion,
  )

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// define entry class
mainClass in assembly := Some("producer.MyKafkaProducer")

assemblyJarName in assembly := "producer.jar"