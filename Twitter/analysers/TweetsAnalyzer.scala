package analysers

import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.xml.dtd.ContentModel.Translator.lang

object TweetsAnalyzer{
  val HASHTAG_COLUMN: String = "hashtag"
  val IS_RETWEET_COLUMN: String = "is_retweet"
  val SOURCE_COLUMN: String = "source"
  val USER_FOLLOWERS: String = "user_followers"
  val USER_NAME: String = "user_name"
  val USER_LOCATION: String = "user_location"
}


class TweetsAnalyzer(sparkSession: SparkSession) {
  /**
   * AGGREGATION
   * @parm df
   * @return Df with columns hashtag, count
   */
  def calculateHashtags(df: Dataset[Row]): Dataset[Row]={
    df.withColumn(TweetsAnalyzer.HASHTAG_COLUMN, explode_outer(col(TweetsAnalyzer.HASHTAG_COLUMN)))
      .groupBy(TweetsAnalyzer.HASHTAG_COLUMN).count()
  }

  /**
   * AGGREGATION
   * @parm df
   * @return Df with columns is_retweet, count
   */
  def calculateIsRetweetCount(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(TweetsAnalyzer.IS_RETWEET_COLUMN).count()
  }

  /**
   * AGGREGATION
   * @parm df
   * @return Df with columns is_retweet, count
   */
  def calculateSourceCount(df: Dataset[Row]): Dataset[Row] = {
    df.groupBy(TweetsAnalyzer.SOURCE_COLUMN).count()
  }

  /**
   * AGGREGATION
   *
   * @parm df
   * @return Df with columns user_location, avg
   */

  def calculateAvgUserFollowersPerLocation(df: Dataset[Row]): Dataset[Row] = {
    df.select(TweetsAnalyzer.USER_NAME, TweetsAnalyzer.USER_FOLLOWERS, TweetsAnalyzer.USER_LOCATION)
      .filter(col(TweetsAnalyzer.USER_NAME).isNotNull)
      .filter(col(TweetsAnalyzer.USER_LOCATION).isNotNull)
      .dropDuplicates(TweetsAnalyzer.USER_NAME)
      .groupBy(TweetsAnalyzer.USER_LOCATION)
      .avg(TweetsAnalyzer.USER_FOLLOWERS)
      .as("avg")
  }

  /**
   * AGGREGATION
   *
   * @param df
   * @return Df with columns user_name, user_location
   */
  def getUsersWithLocation(df: Dataset[Row]): Dataset[Row] = {
    df.select(TweetsAnalyzer.USER_NAME, TweetsAnalyzer.USER_LOCATION)
      .filter(col(TweetsAnalyzer.USER_NAME).isNotNull)
      .filter(col(TweetsAnalyzer.USER_LOCATION).isNotNull)
      .dropDuplicates(TweetsAnalyzer.USER_NAME)
  }

  /**
   * AGGREGATION
   *
   * @param df
   * @param n
   * @return Df with columns hashtag, count
   */

  def getTopNHashtags(df: Dataset[Row], n: Int): Dataset[Row] = {
    df.withColumn(TweetsAnalyzer.HASHTAG_COLUMN, explode_outer(col(TweetsAnalyzer.HASHTAG_COLUMN)))
      .groupBy(TweetsAnalyzer.HASHTAG_COLUMN).count()
      .orderBy(col("count").desc)
      .limit(n)
  }

  /**
   * FILTERING
   *
   * @param df
   * @param minFollowers
   * @return Df filtered by minimum user followers
   */


  def filterByMinUserFollowers(df: Dataset[Row], minFollowers: Int): Dataset[Row] = {
    df.filter(col(TweetsAnalyzer.USER_FOLLOWERS) >= minFollowers)
  }

  /**
   * Calculate the average number of retweets per tweet .
   *
   * @param tweetsDF DataFrame of tweets to analyze.
   * @return Average number of retweets per tweet.
   */
  def calculateAvgRetweetsPerTweet(df: Dataset[Row]): Double = {

    val totalRetweets = {
      df.select(sum(col("retweet_count"))).first().getLong(0)
    }
    val totalTweets = df.count()
    val avgRetweetsPerTweet = totalRetweets / totalTweets.toDouble

    avgRetweetsPerTweet
  }

}
