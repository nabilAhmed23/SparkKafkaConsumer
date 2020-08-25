package com.kafka.consumer.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.{JsonObject, JsonParser, JsonPrimitive}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object Utilities {

  val CLI_TOPIC_SEPARATOR = "\\|"

  val BOOTSTRAP_SERVERS_PROPERTY = "bootstrap.servers"
  val GROUP_ID_PROPERTY = "group.id"
  val KEY_DESERIALIZER_PROPERTY = "key.deserializer"
  val VALUE_DESERIALIZER_PROPERTY = "value.deserializer"
  val AUTO_OFFSET_RESET_PROPERTY = "auto.offset.reset"

  val DATABASE_DRIVER_PROPERTY = "database.driver"
  val DATABASE_URL_PROPERTY = "database.url"
  val DATABASE_TABLE_PROPERTY = "database.table"
  val DATABASE_USERNAME_PROPERTY = "database.username"
  val DATABASE_PASSWORD_PROPERTY = "database.password"

  val DEFAULT_AUTO_OFFSET_RESET = "earliest"

  val TWITTER_DATE_FORMAT = "E MMMM dd hh:mm:ss zzzz yyyy"
  val DATABASE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  private val jsonParser: JsonParser = new JsonParser()

  val twitterStruct: StructType = StructType(List(
    StructField("topic", StringType, nullable = false),
    StructField("topic_alias", StringType, nullable = false),
    StructField("tweet_id", LongType, nullable = false),
    StructField("tweet_text", StringType, nullable = false),
    StructField("tweet_source", StringType, nullable = false),
    StructField("tweet_created_at", TimestampType, nullable = false),
    StructField("tweet_full_text", StringType, nullable = true),
    StructField("user_id", LongType, nullable = false),
    StructField("user_name", StringType, nullable = false),
    StructField("user_screen_name", StringType, nullable = false),
    StructField("quoted_tweet_id", LongType, nullable = true),
    StructField("quoted_tweet_text", StringType, nullable = true),
    StructField("quoted_tweet_source", StringType, nullable = true),
    StructField("quoted_tweet_created_at", TimestampType, nullable = true),
    StructField("quoted_tweet_full_text", StringType, nullable = true),
    StructField("quoted_tweet_user_id", LongType, nullable = true),
    StructField("quoted_tweet_user_name", StringType, nullable = true),
    StructField("quoted_tweet_user_screen_name", StringType, nullable = true)
  ))

  def replaceSpaces(topicName: String): String = {
    topicName.trim.split(" ").mkString("_").trim
  }

  def getConsumerProperties(kafkaProperties: Properties): Properties = {
    synchronized {
      val propertyKeys = kafkaProperties.stringPropertyNames()
      if (!(propertyKeys.contains(BOOTSTRAP_SERVERS_PROPERTY) &&
        propertyKeys.contains(KEY_DESERIALIZER_PROPERTY) &&
        propertyKeys.contains(VALUE_DESERIALIZER_PROPERTY) &&
        propertyKeys.contains(GROUP_ID_PROPERTY))) {
        throw new Exception("Properties file missing one of:" +
          s"\n$BOOTSTRAP_SERVERS_PROPERTY" +
          s"\n$KEY_DESERIALIZER_PROPERTY" +
          s"\n$VALUE_DESERIALIZER_PROPERTY" +
          s"\n$GROUP_ID_PROPERTY" +
          s"\n================")
      }

      val consumerProperties = new Properties()
      consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty(BOOTSTRAP_SERVERS_PROPERTY))
      consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(KEY_DESERIALIZER_PROPERTY))
      consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getProperty(VALUE_DESERIALIZER_PROPERTY))
      consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty(GROUP_ID_PROPERTY))

      if (propertyKeys.contains(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getProperty(AUTO_OFFSET_RESET_PROPERTY))
      } else {
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET)
      }

      println("Consumer properties:")
      consumerProperties.stringPropertyNames().forEach(prop => println(s"Consumer $prop: ${consumerProperties.getProperty(prop)}"))

      consumerProperties
    }
  }

  def getTweetDetailsFromJson(topic: String, topicAlias: String, json: String): Row = {
    try {
      var tmp = getValueFromJson(json, "id_str")
      if (tmp == null) {
        null
      } else {
        val tweetId = tmp.toLong
        val tweetText = getValueFromJson(json, "text")
        val tweetSource = getValueFromJson(json, "source")
        tmp = convertDateFormat(getValueFromJson(json, "created_at"), TWITTER_DATE_FORMAT, DATABASE_DATE_FORMAT)
        val tweetCreatedAt: Timestamp = if (tmp != null) Timestamp.valueOf(tmp) else null

        var quotedTweetId: Long = 0L
        var tweetUserId: Long = 0L
        var tweetUserName: String = null
        var tweetUserScreenName: String = null
        var tweetFullText: String = null
        var quotedTweetJsonString: String = null
        var quotedTweetText: String = null
        var quotedTweetSource: String = null
        var quotedTweetCreatedAt: Timestamp = null
        var quotedUserJsonString: String = null
        var quotedTweetUserId: Long = 0L
        var quotedTweetUserName: String = null
        var quotedTweetUserScreenName: String = null
        var quotedTweetFullText: String = null

        val userJsonString = getValueFromJson(json, "user")
        if (userJsonString != null) {
          tmp = getValueFromJson(userJsonString, "id_str")
          tweetUserId = if (tmp != null) tmp.toLong else 0L
          tweetUserName = getValueFromJson(userJsonString, "name")
          tweetUserScreenName = getValueFromJson(userJsonString, "screen_name")
        }

        val extendedTweetJsonString = getValueFromJson(json, "extended_tweet")
        if (extendedTweetJsonString != null) {
          tweetFullText = getValueFromJson(extendedTweetJsonString, "full_text")
        }

        tmp = getValueFromJson(json, "quoted_status_id_str")
        if (tmp != null) {
          quotedTweetId = tmp.toLong
          quotedTweetJsonString = getValueFromJson(json, "quoted_status")
          if (quotedTweetJsonString != null) {
            quotedTweetText = getValueFromJson(quotedTweetJsonString, "text")
            quotedTweetSource = getValueFromJson(quotedTweetJsonString, "source")
            tmp = convertDateFormat(getValueFromJson(quotedTweetJsonString, "created_at"), TWITTER_DATE_FORMAT, DATABASE_DATE_FORMAT)
            quotedTweetCreatedAt = if (tmp != null) Timestamp.valueOf(tmp) else null
            quotedUserJsonString = getValueFromJson(quotedTweetJsonString, "user")
            if (quotedUserJsonString != null) {
              tmp = getValueFromJson(quotedUserJsonString, "id_str")
              quotedTweetUserId = if (tmp != null) tmp.toLong else 0L
              quotedTweetUserName = getValueFromJson(quotedUserJsonString, "name")
              quotedTweetUserScreenName = getValueFromJson(quotedUserJsonString, "screen_name")
            }
            val extendedQuotedTweetJsonString = getValueFromJson(quotedTweetJsonString, "extended_tweet")
            if (extendedQuotedTweetJsonString != null) {
              quotedTweetFullText = getValueFromJson(extendedQuotedTweetJsonString, "full_text")
            }
          }
        }

        Row(topic,
          topicAlias,
          tweetId,
          tweetText,
          tweetSource,
          tweetCreatedAt,
          tweetFullText,
          tweetUserId,
          tweetUserName,
          tweetUserScreenName,
          quotedTweetId,
          quotedTweetText,
          quotedTweetSource,
          quotedTweetCreatedAt,
          quotedTweetFullText,
          quotedTweetUserId,
          quotedTweetUserName,
          quotedTweetUserScreenName)
      }
    } catch {
      case e: Exception =>
        println("Processing error")

        null
    }
  }

  def getValueFromJson(json: String, key: String): String = {
    try {
      val jsonValue = jsonParser.parse(json).getAsJsonObject.get(key)
      if (jsonValue != null) {
        jsonValue match {
          case _: JsonPrimitive =>
            return jsonValue.getAsString

          case _: JsonObject =>
            return jsonValue.toString

          case _ =>
            return null
        }
      }

      null
    } catch {
      case e: Exception =>
        println(s"$key:: $json\n$e")

        null
    }
  }

  def convertDateFormat(dateString: String, srcFormat: String, destFormat: String): String = {
    try {
      new SimpleDateFormat(destFormat).format(new SimpleDateFormat(srcFormat).parse(dateString))
    } catch {
      case e: Exception =>
        println(s"Error in processing date '$dateString'")

        null
    }
  }

  def getDatabaseProperties(properties: Properties): Properties = {
    val databaseProperties = new Properties()
    databaseProperties.setProperty("user", properties.getProperty(DATABASE_USERNAME_PROPERTY))
    databaseProperties.setProperty("password", properties.getProperty(DATABASE_PASSWORD_PROPERTY))

    databaseProperties
  }
}
