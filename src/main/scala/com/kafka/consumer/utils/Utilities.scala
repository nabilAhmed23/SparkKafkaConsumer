package com.kafka.consumer.utils

import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.JsonParser
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

  def getTweetDetailsFromJson(json: String): Array[Any] = {
    val tweetId = getValueFromJson(json, "id_str")
    println(s"tweet id: $tweetId")
    if (tweetId == null) {
      null
    } else {
      val tweetText = getValueFromJson(json, "text")
      println(s"text: $tweetText")
      val tweetSource = getValueFromJson(json, "source")
      println(s"source: $tweetSource")
      val quotedTweetId = getValueFromJson(json, "quoted_status_id_str")
      println(s"quoted tweet: $quotedTweetId")
      val tweetCreatedAt = convertDateFormat(getValueFromJson(json, "created_at"), TWITTER_DATE_FORMAT, DATABASE_DATE_FORMAT)
      println(s"created at: $tweetCreatedAt")

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
      println(s"================\nuser: $userJsonString")
      if (userJsonString != null) {
        tweetUserId = getValueFromJson(userJsonString, "id_str")
        tweetUserName = getValueFromJson(userJsonString, "name")
        tweetUserScreenName = getValueFromJson(userJsonString, "screen_name")
      }
      println(s"user id: $tweetUserId")
      println(s"user name: $tweetUserName")
      println(s"user screen name: $tweetUserScreenName")

      val extendedTweetJsonString = getValueFromJson(json, "extended_tweet")
      println(s"================\nextended tweet: $extendedTweetJsonString")
      if (extendedTweetJsonString != null) {
        tweetFullText = getValueFromJson(extendedTweetJsonString, "full_text")
      }
      println(s"================\nfull text: $tweetFullText")

      if (quotedTweetId != null) {
        quotedTweetJsonString = getValueFromJson(json, "quoted_status")
        println(s"================\nquoted tweet: $quotedTweetJsonString")
        if (quotedTweetJsonString != null) {
          quotedTweetText = getValueFromJson(quotedTweetJsonString, "text")
          println(s"quoted text: $quotedTweetText")
          quotedTweetSource = getValueFromJson(quotedTweetJsonString, "source")
          println(s"quoted source: $quotedTweetSource")
          quotedTweetCreatedAt = convertDateFormat(getValueFromJson(quotedTweetJsonString, "created_at"), TWITTER_DATE_FORMAT, DATABASE_DATE_FORMAT)
          println(s"quoted created at: $quotedTweetCreatedAt")
          quotedUserJsonString = getValueFromJson(quotedTweetJsonString, "user")
          println(s"================\nquoted user: $quotedUserJsonString")
          if (quotedUserJsonString != null) {
            quotedTweetUserId = getValueFromJson(quotedUserJsonString, "id_str")
            println(s"quoted user id: $quotedTweetUserId")
            quotedTweetUserName = getValueFromJson(quotedUserJsonString, "name")
            println(s"quoted user name: $quotedTweetUserName")
            quotedTweetUserScreenName = getValueFromJson(quotedUserJsonString, "screen_name")
            println(s"quoted user screen name: $quotedTweetUserScreenName")
          }
          val extendedQuotedTweetJsonString = getValueFromJson(quotedTweetJsonString, "extended_tweet")
          println(s"================\nquoted extended tweet: $quotedUserJsonString")
          if (extendedQuotedTweetJsonString != null) {
            quotedTweetFullText = getValueFromJson(extendedQuotedTweetJsonString, "full_text")
          }
          println(s"quoted text full: $quotedTweetFullText")
        }
      }
      println("Processed tweet:")
      println(s"$tweetId" +
        s"\n$tweetText" +
        s"\n$tweetSource" +
        s"\n$quotedTweetId" +
        s"\n$tweetCreatedAt" +
        s"\n$tweetUserId" +
        s"\n$tweetUserName" +
        s"\n$tweetUserScreenName" +
        s"\n$tweetFullText" +
        s"\n$quotedTweetText" +
        s"\n$quotedTweetSource" +
        s"\n$quotedTweetCreatedAt" +
        s"\n$quotedTweetUserId" +
        s"\n$quotedTweetUserName" +
        s"\n$quotedTweetUserScreenName" +
        s"\n$quotedTweetFullText")

      Array(tweetId.toInt,
        tweetText,
        tweetSource,
        if (quotedTweetId != null) quotedTweetId.toInt else quotedTweetId,
        tweetCreatedAt,
        if (tweetUserId != null) tweetUserId.toInt else tweetUserId,
        tweetUserName,
        tweetUserScreenName,
        tweetFullText,
        quotedTweetText,
        quotedTweetSource,
        quotedTweetCreatedAt,
        if (quotedTweetUserId != null) quotedTweetUserId.toInt else quotedTweetUserId,
        quotedTweetUserName,
        quotedTweetUserScreenName,
        quotedTweetFullText)
    }
  }

  def getValueFromJson(json: String, key: String): String = {
    try {
      JsonParser.parseString(json).getAsJsonObject.get(key).getAsString
    } catch {
      case e: Exception =>
        println(s"$key:: $json")
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
