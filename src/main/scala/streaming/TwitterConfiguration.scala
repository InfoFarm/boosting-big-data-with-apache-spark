package streaming

object TwitterConfiguration {

  /*
   * Set some required parameters for connecting to a Twitter stream
   */
  def configureSecurity() = {
    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")
  }
}