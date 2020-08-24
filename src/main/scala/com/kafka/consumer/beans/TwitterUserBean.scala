package com.kafka.consumer.beans

class TwitterUserBean(private var userId: Long = 0L,
                      private var userName: String = null,
                      private var userScreenName: String = null)
  extends java.io.Serializable {

  def getUserId: Long = {
    userId
  }

  def setUserId(id: Long): Unit = {
    userId = id
  }

  def getUserName: String = {
    userName
  }

  def setUserName(name: String): Unit = {
    userName = name
  }

  def getUserScreenName: String = {
    userScreenName
  }

  def setUserScreenName(name: String): Unit = {
    userScreenName = name
  }

  override def toString: String = {
    s"userId: $userId, " +
      s"userName: $userName, " +
      s"userScreenName: $userScreenName"
  }
}
