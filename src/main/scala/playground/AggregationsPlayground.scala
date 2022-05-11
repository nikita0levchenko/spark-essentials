package playground

import org.apache.spark.sql.functions.{
  approx_count_distinct,
  avg,
  col,
  count,
  countDistinct,
  max,
  mean,
  min,
  stddev,
  sum
}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AggregationsPlayground extends App {
  val spark = SparkSession
    .builder()
    .appName("Aggregation application")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDf: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDf.printSchema()
  moviesDf.show()

  // counting

  val countedGenre: DataFrame =
    moviesDf.select(count(col("Major_Genre"))) // all the values except null
  countedGenre.show()

  // as sql table
  moviesDf.registerTempTable("movies")
  val sqlCountedGenre: DataFrame =
    spark.sql("SELECT COUNT(Major_Genre) FROM movies")
  sqlCountedGenre.printSchema()
  sqlCountedGenre.show()

  //with selectExpr
  //counting all
  val selectExprCountedGenre = moviesDf.selectExpr("COUNT(Major_Genre)")
  selectExprCountedGenre.printSchema()
  selectExprCountedGenre.show()
  moviesDf.selectExpr("COUNT(*)").show() // count all the rows, included null

  //count distinct
  val distinctGenres: DataFrame =
    moviesDf.select(countDistinct(col("Major_Genre")))
  distinctGenres.printSchema()
  distinctGenres.show()

  // approximate count
  val approximateCountedGenres: DataFrame =
    moviesDf.select(
      approx_count_distinct(col("Major_Genre"))
    ) // good operation if you have a deal with very large data
  approximateCountedGenres.printSchema()
  approximateCountedGenres.show()

  // min and max
  val maxIMDB: DataFrame = moviesDf.select(max(col("IMDB_Rating")))
  maxIMDB.printSchema()
  maxIMDB.show()

  val sqlMaxIMDB: DataFrame = moviesDf.selectExpr("MAX(IMDB_Rating)")
  sqlMaxIMDB.printSchema()
  sqlMaxIMDB.show()

  val minProductionBudget: DataFrame = moviesDf.select(min(col("IMDB_Rating")))
  minProductionBudget.printSchema()
  minProductionBudget.show()

  val sqlMinIMDB: DataFrame = moviesDf.selectExpr("MIN(IMDB_Rating)")
  sqlMinIMDB.printSchema()
  sqlMinIMDB.show()

  // sum
  val allSummedBudget: DataFrame =
    moviesDf.select(sum(col("Production_Budget")))
  allSummedBudget.printSchema()
  allSummedBudget.show()

  val sqlSummedBudget: DataFrame = moviesDf.selectExpr("SUM(Production_Budget)")
  sqlSummedBudget.printSchema()
  sqlSummedBudget.show()

  // average(avg)
  val avgRTRating: DataFrame =
    moviesDf.select(avg(col("Rotten_Tomatoes_Rating")))
  avgRTRating.printSchema()
  avgRTRating.show()

  val sqlAvgRTRating: DataFrame =
    moviesDf.selectExpr("AVG(Rotten_Tomatoes_Rating)")
  sqlAvgRTRating.printSchema()
  sqlAvgRTRating.show()

  //data science
  moviesDf
    .select(
      mean(col("Rotten_Tomatoes_Rating")), // equals avg
      stddev(
        col("Rotten_Tomatoes_Rating")
      ) // how strong values spread through dataframe
    )
    .show()

  // Grouping
  val groupByGenre: DataFrame =
    moviesDf.groupBy(col("Major_Genre")).count() // groupBy includes a null
  groupByGenre.printSchema()
  groupByGenre
    .show() // on SQL: SELECT COUNT(*) FROM MOVIESDF GROUP BY Major_Genre;

  val avgRatingByGenre: DataFrame =
    moviesDf.groupBy(col("Major_Genre")).avg("IMDB_Rating")
  avgRatingByGenre.printSchema()
  avgRatingByGenre.show()

  val aggregationedMovies: DataFrame = moviesDf
    .groupBy("Major_Genre")
    .agg(
      count("*").as("N_of_movies"),
      avg("IMDB_Rating").as("Average_IMDB_rating")
    )
    .orderBy(col("Average_IMDB_rating"))
  aggregationedMovies.printSchema()
  aggregationedMovies.show()

  /** Exercises
    * 1) Sum up ALL the profits of ALL the movies int the DF
    * 2) Count how many distinct directors we have
    * 3) Show the mean and standard deviation of US gross for the movies
    * 4) Compute average IMDB rating and average US gross revenue per director
    */

  // Exercise 1
  val sumUpAllMovies: DataFrame = moviesDf.select(
    sum(col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
      .as("Total_gross")
  )
  sumUpAllMovies.printSchema()
  sumUpAllMovies.show()

  // Exercise 2
  val distinctDirectors: DataFrame =
    moviesDf.select(countDistinct(col("Director")))
  distinctDirectors.printSchema()
  distinctDirectors.show()

  // Exercise 3
  val statisticsPerMovie: DataFrame = moviesDf
    .select(
      mean("US_Gross"),
      stddev("US_Gross")
    )

  statisticsPerMovie.printSchema()
  statisticsPerMovie.show()

  // Exercise 4
  val statisticsPerDirector: DataFrame = moviesDf
    .groupBy("Director")
    .agg(
      avg("US_Gross").as("US_gross_avg"),
      avg("IMDB_Rating").as("IMDB_avg_rating")
    )
    .orderBy(col("IMDB_avg_rating").desc_nulls_last)

  statisticsPerDirector.printSchema()
  statisticsPerDirector.show()

}
