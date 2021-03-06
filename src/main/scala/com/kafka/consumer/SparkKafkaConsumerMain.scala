package com.kafka.consumer

import java.io.FileReader
import java.util.Properties

import com.kafka.consumer.utils.Utilities
import org.apache.spark.sql.SparkSession

object SparkKafkaConsumerMain {

  def main(args: Array[String]): Unit = {
    try {
      val session = SparkSession.builder()
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
      session.sparkContext.setLogLevel("ERROR")
      val kafkaProperties = new Properties()

      if (args.length != 2) {
        throw new Exception(s"Incorrect number of arguments. Expected 2 (properties file, topics), got ${args.length}")
      }
      kafkaProperties.load(new FileReader(args(0).trim))
      val kafkaTopics = args(1).trim.split(Utilities.CLI_TOPIC_SEPARATOR)

      for (topic <- kafkaTopics) {
        val topicNew = Utilities.replaceSpaces(topic)
        val databaseConsumer = new SparkKafkaConsumer(session, kafkaProperties, topic, topicNew)
        databaseConsumer.setName(s"$topicNew-Database-Consumer-Thread")
        databaseConsumer.start()
      }
    } catch {
      case e: Exception => println(e)
    }
  }
}
