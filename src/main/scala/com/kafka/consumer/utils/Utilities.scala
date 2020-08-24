package com.kafka.consumer.utils

import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.{JsonObject, JsonParser, JsonPrimitive}
import com.kafka.consumer.beans.{TwitterBean, TwitterTweetBean, TwitterUserBean}
import org.apache.kafka.clients.consumer.ConsumerConfig

object Utilities {

  val CLI_TOPIC_SEPARATOR = "\\|"

  val BOOTSTRAP_SERVERS_PROPERTY = "bootstrap.servers"
  val GROUP_ID_PROPERTY = "group.id"
  val KEY_DESERIALIZER_PROPERTY = "key.deserializer"
  val VALUE_DESERIALIZER_PROPERTY = "value.deserializer"

  val AUTO_OFFSET_RESET_PROPERTY = "auto.offset.reset"

  val DEFAULT_AUTO_OFFSET_RESET = "earliest"

  val TWITTER_DATE_FORMAT = "E MMMM dd hh:mm:ss zzzz yyyy"
  val DATABASE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss Z"

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

  def getTweetDetailsFromJson(json: String): TwitterBean = {
    try {
      val tweetId = getValueFromJson(json, "id_str")
      if (tweetId == null) {
        null
      } else {
        val tweetText = getValueFromJson(json, "text")
        val tweetSource = getValueFromJson(json, "source")
        val quotedTweetId = getValueFromJson(json, "quoted_status_id_str")
        val tweetCreatedAt = convertDateFormat(getValueFromJson(json, "created_at"), TWITTER_DATE_FORMAT, DATABASE_DATE_FORMAT)

        var tweetUserId: String = null
        var tweetUserName: String = null
        var tweetUserScreenName: String = null
        var tweetFullText: String = null
        var quotedTweetJsonString: String = null
        var quotedTweetText: String = null
        var quotedTweetSource: String = null
        var quotedTweetCreatedAt: String = null
        var quotedUserJsonString: String = null
        var quotedTweetUserId: String = null
        var quotedTweetUserName: String = null
        var quotedTweetUserScreenName: String = null
        var quotedTweetFullText: String = null

        val userJsonString = getValueFromJson(json, "user")
        if (userJsonString != null) {
          tweetUserId = getValueFromJson(userJsonString, "id_str")
          tweetUserName = getValueFromJson(userJsonString, "name")
          tweetUserScreenName = getValueFromJson(userJsonString, "screen_name")
        }

        val extendedTweetJsonString = getValueFromJson(json, "extended_tweet")
        if (extendedTweetJsonString != null) {
          tweetFullText = getValueFromJson(extendedTweetJsonString, "full_text")
        }

        if (quotedTweetId != null) {
          quotedTweetJsonString = getValueFromJson(json, "quoted_status")
          if (quotedTweetJsonString != null) {
            quotedTweetText = getValueFromJson(quotedTweetJsonString, "text")
            quotedTweetSource = getValueFromJson(quotedTweetJsonString, "source")
            quotedTweetCreatedAt = convertDateFormat(getValueFromJson(quotedTweetJsonString, "created_at"), TWITTER_DATE_FORMAT, DATABASE_DATE_FORMAT)
            quotedUserJsonString = getValueFromJson(quotedTweetJsonString, "user")
            if (quotedUserJsonString != null) {
              quotedTweetUserId = getValueFromJson(quotedUserJsonString, "id_str")
              quotedTweetUserName = getValueFromJson(quotedUserJsonString, "name")
              quotedTweetUserScreenName = getValueFromJson(quotedUserJsonString, "screen_name")
            }
            val extendedQuotedTweetJsonString = getValueFromJson(quotedTweetJsonString, "extended_tweet")
            if (extendedQuotedTweetJsonString != null) {
              quotedTweetFullText = getValueFromJson(extendedQuotedTweetJsonString, "full_text")
            }
          }
        }

        val tweetBean = new TwitterTweetBean(tweetId.toLong, tweetText, tweetSource, tweetCreatedAt, tweetFullText)
        val userBean = if (tweetUserId == null) new TwitterUserBean() else
          new TwitterUserBean(tweetUserId.toLong, tweetUserName, tweetUserScreenName)
        val quotedTweetBean = if (quotedTweetId == null) new TwitterTweetBean() else
          new TwitterTweetBean(quotedTweetId.toLong, quotedTweetText, quotedTweetSource, quotedTweetCreatedAt, quotedTweetFullText)
        val quotedUserBean = if (quotedTweetUserId == null) new TwitterUserBean() else
          new TwitterUserBean(quotedTweetUserId.toLong, quotedTweetUserName, quotedTweetUserScreenName)
        val twitterBean = new TwitterBean(tweetBean, userBean, quotedTweetBean, quotedUserBean)
        println(twitterBean)

        twitterBean
      }
    } catch {
      case e: Exception => println("Processing error")
        null
    }
  }

  def getValueFromJson(json: String, key: String): String = {
    try {
      val jsonValue = JsonParser.parseString(json).getAsJsonObject.get(key)
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
        println(s"$key:: $json \n$e")
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
}
