package com.kafka.consumer.beans

class TwitterTweetBean(private var tweetId: Long = 0L,
                       private var tweetText: String = null,
                       private var tweetSource: String = null,
                       private var tweetCreatedAt: String = null,
                       private var tweetFullText: String = null)
  extends java.io.Serializable {

  def getTweetId: Long = {
    tweetId
  }

  def setTweetId(id: Long): Unit = {
    tweetId = id
  }

  def getTweetText: String = {
    tweetText
  }

  def setTweetText(text: String): Unit = {
    tweetText = text
  }

  def getTweetSource: String = {
    tweetSource
  }

  def setTweetSource(source: String): Unit = {
    tweetSource = source
  }

  def getTweetCreatedAt: String = {
    tweetCreatedAt
  }

  def setTweetCreatedAt(createdAt: String): Unit = {
    tweetCreatedAt = createdAt
  }

  def getTweetFullText: String = {
    tweetFullText
  }

  def setTweetFullText(text: String): Unit = {
    tweetFullText = text
  }

  override def toString: String = {
    s"tweetId: $tweetId, " +
      s"\ntweetText: $tweetText, " +
      s"\ntweetSource: $tweetSource, " +
      s"\ntweetCreatedAt: $tweetCreatedAt, " +
      s"\ntweetFullText: $tweetFullText"
  }
}
