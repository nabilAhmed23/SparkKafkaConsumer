name := "SparkKafkaConsumer"

version := "1.0"

scalaVersion := "2.12.11"
mainClass in Compile := Some("com.kafka.consumer.SparkKafkaConsumerMain")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.0.0",
  "org.apache.spark" % "spark-sql_2.12" % "3.0.0",
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
  "com.google.code.gson" % "gson" % "2.2.4"
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
