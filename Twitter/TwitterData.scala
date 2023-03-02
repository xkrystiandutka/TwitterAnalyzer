import org.apache.spark.sql.{Dataset, Row, SparkSession}

object TwitterData {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Twitter")
      .master("local[*]")
      .getOrCreate()

    val covid19DF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("covid19_tweets.csv")

    val financialDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("financial.csv")

    val grammyDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("grammys_tweets.csv")

    covid19DF.show(false)
    covid19DF.printSchema()

    financialDF.show(false)
    financialDF.printSchema()

    grammyDF.show(false)
    grammyDF.printSchema()
  }
}
