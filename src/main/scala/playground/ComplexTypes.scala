package playground

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object ComplexTypes extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("complex-types")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // dates
  val moviesWithReleaseDatesDF: DataFrame = moviesDF
    .select(
      col("Title"),
      to_date(col("Release_Date"), "dd-MMM-yy").as(
        "Actual_release"
      ) // conversion
    )
    .withColumn("today", current_date()) // current date
    .withColumn("right_now", current_timestamp()) // now
    .withColumn(
      "movie_age",
      datediff( // difference between dates | date_add, date_sub
        col("today"),
        col("Actual_release")
      ) / 365
    )

  moviesWithReleaseDatesDF
    .select("*")
    .where(col("Actual_release").isNull)
    .show()

  /** Exercise
    * 1) How do we deal with dates in multiple formats?
    * 2) Read the stocks DF and parse the dates
    */

  /* Exercise 1:
  1) parse the DF multiple times, then union the small DF's
  2) if data with strange format is small - you can just ignore it
   */
  // Exercise 2
  val stocksDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true") // is important for csv's
    .csv("src/main/resources/data/stocks.csv")

  val processedStocksDF: DataFrame = stocksDF
    .withColumn("processed_date", to_date(col("date"), "MMM dd yyyy"))
  processedStocksDF.show()

  // Structures

  val moviesWithStructProfitDF: DataFrame = moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales")).as(
        "profit"
      )
    )
  moviesWithStructProfitDF.printSchema()
  moviesWithStructProfitDF.show()

  // 1 - with col operators
  // select the sub-fields
  moviesWithStructProfitDF
    .select(col("Title"), col("profit").getField("US_Gross").as("US_Profit"))
    .show()
  moviesWithStructProfitDF.select("Title", "profit.US_Gross").show()

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross, US_DVD_Sales) as profit")
    .selectExpr("profit.US_Gross as US_profit")
    .show()

  // Arrays

  val moviesWithWords: DataFrame =
    moviesDF.select(
      col("Title"),
      split(col("Title"), " |,").as("Title_words")
    ) // Array of words

  moviesWithWords.printSchema()
  moviesWithWords.show()

  moviesWithWords
    .select(
      col("Title"),
      expr("Title_words[0]"),
      size(col("Title_words")),
      array_contains(col("Title_words"), "Love")
    )
    .show()
}
