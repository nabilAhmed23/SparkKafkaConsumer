package com.kafka.consumer.beans

class TwitterBean(private var tweetBean: TwitterTweetBean = new TwitterTweetBean(),
                  private var userBean: TwitterUserBean = new TwitterUserBean(),
                  private var quotedTweetBean: TwitterTweetBean = null,
                  private var quotedUserBean: TwitterUserBean = null)
  extends java.io.Serializable {

  def getTweetBean: TwitterTweetBean = {
    tweetBean
  }

  def setTweetBean(bean: TwitterTweetBean): Unit = {
    tweetBean = bean
  }

  def getUserBean: TwitterUserBean = {
    userBean
  }

  def setUserBean(bean: TwitterUserBean): Unit = {
    userBean = bean
  }

  def getQuotedTweetBean: TwitterTweetBean = {
    quotedTweetBean
  }

  def setQuotedTweetBean(bean: TwitterTweetBean): Unit = {
    quotedTweetBean = bean
  }

  def getQuotedUserBean: TwitterUserBean = {
    quotedUserBean
  }

  def setQuotedUserBean(bean: TwitterUserBean): Unit = {
    quotedUserBean = bean
  }

  override def toString: String = {
    if (quotedTweetBean != null && quotedUserBean != null) {
      s"\nTweet details::" +
        s"\n\t${tweetBean.toString}" +
        s"\nUser details::" +
        s"\n\t${userBean.toString}" +
        s"\nQuoted tweet details::" +
        s"\n\t${quotedTweetBean.toString}" +
        s"\nQuoted user details::" +
        s"\n\t${quotedUserBean.toString}"
    } else {
      s"\nTweet details::" +
        s"\n\t${tweetBean.toString}" +
        s"\nUser details::" +
        s"\n\t${userBean.toString}"
    }
  }
}
