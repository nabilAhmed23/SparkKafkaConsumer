package com.kafka.consumer

import java.time.Duration
import java.util
import java.util.Properties

import com.kafka.consumer.beans.TwitterBean
import com.kafka.consumer.utils.Utilities
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class SparkKafkaConsumer(var session: SparkSession,
                         var consumerProperties: Properties,
                         var topicAlias: String)
  extends Thread {

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
            var topicRecords = session.sparkContext.emptyRDD[TwitterBean]
            records.forEach(record => {
              println(record.value())
              val tweetDetails = Utilities.getTweetDetailsFromJson(record.value())
              if (tweetDetails != null) {
                println(s"$topicAlias:: Consumer received message k at: ${record.timestamp()}\n================")
                topicRecords = topicRecords.union(session.sparkContext.parallelize(List(tweetDetails)))
              }
            })
            if (topicRecords.count() > 0) {
              println("================Twitter Bean Dataframe================")
              session.createDataFrame(topicRecords, classOf[TwitterBean]).printSchema()
              //              consumer.commitSync()
            }
          } catch {
            case e: NoSuchMethodError => println(s"$topicAlias:: Error in processing $e")
          }
        }
      }
    }
  }

  def getTwitterStruct: StructType = {
    StructType(List(
      StructField("tweetId", LongType, nullable = false),
      StructField("tweetText", StringType, nullable = false),
      StructField("tweetSource", StringType, nullable = false),
      StructField("tweetCreatedAt", StringType, nullable = false),
      StructField("tweetFullText", StringType, nullable = false),
      StructField("userId", LongType, nullable = false),
      StructField("userName", StringType, nullable = false),
      StructField("userScreenName", StringType, nullable = false),
      StructField("quotedTweetId", LongType, nullable = true),
      StructField("quotedTweetText", StringType, nullable = true),
      StructField("quotedTweetSource", StringType, nullable = true),
      StructField("quotedTweetCreatedAt", StringType, nullable = true),
      StructField("quotedTweetFullText", StringType, nullable = true),
      StructField("quotedTweetUserId", LongType, nullable = true),
      StructField("quotedTweetUserName", StringType, nullable = true),
      StructField("quotedTweetUserScreenName", StringType, nullable = true)
    ))
  }
}
