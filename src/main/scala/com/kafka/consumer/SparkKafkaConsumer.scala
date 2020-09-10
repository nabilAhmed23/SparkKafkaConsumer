package com.kafka.consumer

import java.time.Duration
import java.util
import java.util.Properties

import com.kafka.consumer.utils.Utilities
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

class SparkKafkaConsumer(var session: SparkSession,
                         var kafkaProperties: Properties,
                         var topic: String,
                         var topicAlias: String)
  extends Thread {

  override def run(): Unit = {
    var tweetList = List.empty[Row]
    val jdbcDriver = kafkaProperties.getProperty(Utilities.DATABASE_DRIVER_PROPERTY)
    val jdbcUrl = kafkaProperties.getProperty(Utilities.DATABASE_URL_PROPERTY)
    val jdbcTable = kafkaProperties.getProperty(Utilities.DATABASE_TABLE_PROPERTY)
    val jdbcProperties = Utilities.getDatabaseProperties(kafkaProperties)

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](Utilities.getConsumerProperties(kafkaProperties))
    consumer.subscribe(util.Arrays.asList(topicAlias))
    println(s"$topicAlias:: Consumer subscribed to topics================")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      synchronized {
        println(s"$topicAlias::Caught consumer shutdown hook================")
        try {
          if (tweetList.nonEmpty) {
            println(s"$topicAlias:: Writing and committing ${tweetList.length} record(s) to database")
            session.createDataFrame(session.sparkContext.parallelize(tweetList), Utilities.twitterStruct)
              .write
              .mode(SaveMode.Append)
              .option("driver", jdbcDriver)
              .jdbc(url = jdbcUrl,
                table = jdbcTable,
                connectionProperties = jdbcProperties)
            consumer.commitSync()
            println(s"$topicAlias:: Write and commit complete")
          }
          consumer.close()
        } catch {
          case _: Exception =>
            session.stop()
            println(s"$topicAlias:: Consumer spark session terminated================")
        }
      }
    }))

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(5))
      if (!records.isEmpty) {
        synchronized {
          try {
            println(s"$topicAlias:: Processing consumer records================")
            records.forEach(record => {
              val tweetDetails = Utilities.getTweetDetailsFromJson(topic, topicAlias, record.value())
              if (tweetDetails != null) {
                println(s"$topicAlias:: Consumer received message at: ${record.timestamp()}")
                tweetList = tweetList :+ tweetDetails
              }
              if (tweetList.length > 16) {
                println(s"$topicAlias:: Writing and committing ${tweetList.length} records to database")
                session.createDataFrame(session.sparkContext.parallelize(tweetList), Utilities.twitterStruct)
                  .write
                  .mode(SaveMode.Append)
                  .option("driver", jdbcDriver)
                  .jdbc(url = jdbcUrl,
                    table = jdbcTable,
                    connectionProperties = jdbcProperties)
                consumer.commitSync()
                tweetList = List.empty[Row]
                println(s"$topicAlias:: Write and commit complete")
              }
            })
          } catch {
            case e: NoSuchMethodError => println(s"$topicAlias:: Error in processing $e")
            case e: Exception => println(e)
          }
        }
      }
    }
  }
}
