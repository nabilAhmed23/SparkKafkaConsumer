package com.kafka.consumer

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.spark.sql.SparkSession

class SparkKafkaConsumer(session: SparkSession, consumerProperties: Properties, topicAlias: String) extends Thread {

  override def run(): Unit = {
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)
    consumer.subscribe(util.Arrays.asList(topicAlias))
    println(s"$topicAlias:: Consumer subscribed to topics================")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println(s"$topicAlias::Caught consumer shutdown hook================")
      try {
        consumer.close()
      } catch {
        case e: Exception =>
          session.stop()
          println(s"$topicAlias:: Consumer spark session terminated================")
      }
    }))

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(5))
      if (!records.isEmpty) {
        synchronized {
          try {
            println(s"$topicAlias:: Processing consumer records================")
            var topicRecords = session.sparkContext.emptyRDD[Any]
            records.forEach(record => {
              println(record.value())
              /*val tweetDetails = Utilities.getTweetDetailsFromJson(record.value())
              if (tweetDetails != null) {
                for (tweet <- tweetDetails) {
                  println(tweet)
                }
                val topicRecord = session.sparkContext.parallelize(tweetDetails)
                topicRecords = topicRecords.union(topicRecord)
                println(s"$topicAlias:: Consumer received message at: ${record.timestamp()}\n================")
              }*/
            })
          } catch {
            case e: NoSuchMethodError => println(s"$topicAlias:: Error in processing $e")
          }
        }
      }
    }
  }
}
