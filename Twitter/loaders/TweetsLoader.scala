package loaders

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TweetsLoader {
  val COVID_LABEL: String = "covid"
  val FINANCE_LABEL: String = "finance"
  val GRAMMYS_LABEL: String = "grammys"
}

class TweetsLoader(sparkSession: SparkSession) {

  def loadAllTweets(): Dataset[Row]={
    val covidDF: Dataset[Row] = loadCovid()
    val financeDF: Dataset[Row] = loadFinance()
    val grammysDF: Dataset[Row] = loadGrammys()

    covidDF.unionByName(financeDF, true)
      .unionByName(grammysDF, true)
  }

  def loadCovid(): Dataset[Row] = {
    sparkSession.read
      .option("header", "true")
      .csv("covid19_tweets.csv")
      .withColumn("category", lit(TweetsLoader.COVID_LABEL))
      .na.drop()
  }

  def loadFinance(): Dataset[Row] = {
    sparkSession.read
      .option("header", "true")
      .csv("financial.csv")
      .withColumn("category", lit(TweetsLoader.FINANCE_LABEL))
      .na.drop()
  }

  def loadGrammys(): Dataset[Row] = {
    sparkSession.read
      .option("header", "true")
      .csv("GRAMMYs_tweets.csv")
      .withColumn("category", lit(TweetsLoader.GRAMMYS_LABEL))
      .na.drop()
  }
}
