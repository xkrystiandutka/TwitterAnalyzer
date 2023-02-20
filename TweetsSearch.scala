package analysers

import org.apache.spark.sql.functions.{array_intersect, col, lit, size, split}
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

object TweetsSearch {
  val TEXT: String = "text"
  val USER_LOCATION: String = "user_location"
}
class TweetsSearch(sparkSession: SparkSession) {

  def searchByKeyWord(keyWord: String)(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(TweetsSearch.TEXT).contains(keyWord))
  }

  def searchByKeyWords(keyWords: Seq[String])(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("keyWordsResult", array_intersect(functions.split(col(TweetsSearch.TEXT), " "), split(lit(keyWords.mkString(",")), ",")))
      .filter(!(col("keyWordsResult").isNull.or(size(col("keyWordsResult")).equalTo(0))))
      .drop("keyWordsResult")
  }

  def onlyInLocation(location: String)(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(TweetsSearch.USER_LOCATION).equalTo(location))
  }

  def searchByKeyWordAndLocation(keyWord: String, location: String)(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col(TweetsSearch.TEXT).contains(keyWord) && col(TweetsSearch.USER_LOCATION).equalTo(location))
  }

  /**
   * FILTERING
   *
   * @param fromDate the start date of the range (inclusive)
   * @param toDate   the end date of the range (inclusive)
   * @param df       the dataset to filter
   * @return the filtered dataset
   */
  def filterByDateRange(fromDate: String, toDate: String)(df: Dataset[Row]): Dataset[Row] = {
    df.filter(col("date").geq(fromDate) && col("date").leq(toDate))
  }
}