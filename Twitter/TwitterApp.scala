import analysers.{TweetsAnalyzer, TweetsSearch}
import cleaners.TweetsCleaner
import loaders.TweetsLoader
import org.apache.spark.sql.functions.{col, lit, lower}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object TwitterApp {
  def main(args: Array[String]): Unit={
    val spark: SparkSession = SparkSession.builder()
      .appName("Twitter")
      .master("local[*]")
      .getOrCreate()

      val tweetsLoader: TweetsLoader = new TweetsLoader(spark)
      val tweetsCleaner: TweetsCleaner = new TweetsCleaner(spark)
      val tweetsSearch: TweetsSearch = new TweetsSearch(spark)
      val tweetsAnalyzer: TweetsAnalyzer = new TweetsAnalyzer(spark)

    import tweetsSearch._

    // Import and clean
      val tweetsDF:Dataset[Row] = tweetsLoader.loadAllTweets().cache()
      val tweetsCleanedDF: Dataset[Row] = tweetsCleaner.cleanAllTweets(tweetsDF)

    // Search specific

    val trumpTweetsDF: Dataset[Row] = tweetsCleanedDF.transform(searchByKeyWord("Trump"))
      .transform(onlyInLocation("United States"))

    // Analytics

    val sourceCount: Dataset[Row] = tweetsAnalyzer.calculateSourceCount(trumpTweetsDF)
    sourceCount.show()

    val hashtagsDF = tweetsDF
      .filter(lower(col("text")).contains("trump"))
      .filter(col("user_location") === lit("United States"))
    hashtagsDF.show()

    val fromDate = "2020-07-25"
    val toDate = "2020-07-26"
    val filteredData = filterByDateRange(fromDate, toDate)(trumpTweetsDF)
    filteredData.show()
  }
}