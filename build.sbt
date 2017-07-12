name := "kafkaexample"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.1"

mainClass in Compile := Some("HelloWorld")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe.play" %% "play-json" % "2.4.0-M2"
)
        