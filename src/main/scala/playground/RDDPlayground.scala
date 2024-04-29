package playground

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.Source

object RDDPlayground extends App {

  val spark = SparkSession
    .builder()
    .appName("Introduction to RDD")
    .config("spark.master", "local")
    .getOrCreate()

  val sparkContext = spark.sparkContext

  // 1 parallelize existing collections
  val numbers = 1 to 100000
  val numbersRDD = sparkContext.parallelize(numbers)

  // 2 read from file
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(fileName: String) = Source
    .fromFile(fileName)
    .getLines()
    .drop(1)
    .map(line => line.split(","))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    .toList

  val stocksRDD = sparkContext
    .parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2.1 - reading from file
  val stocksRDD2 = sparkContext
    .textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS =
    stocksDF
      .as[StockValue] // to keep type information need to convert into dataset Dataset[StockValue] vs Dataset[Row]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF: DataFrame = numbersRDD.toDF("numbers")

  // RDD -> DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)

  // Transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  // counting
  val countedStocks = stocksRDD.count() // eager action

  // distinct
  val distinctCountStockRDD = stocksRDD.map(_.symbol).distinct() // lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan((stockA, stockB) => stockA.price <= stockB.price)

  val minStockRDD = stocksRDD.min()
  println(minStockRDD)

  // reduce
  val reducedNumbersRDD = numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD =
    stocksRDD.groupBy(_.symbol) // - very expensive operation

  // Partitioning
  val repartitionedRDD = stocksRDD.repartition(30)
  repartitionedRDD
    .toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet(
      "src/main/resources/data/repartitioned_stocks"
    ) // repartitioning is expensive because of shuffle

  // Best practice for partition size is 10 - 100 Mb

  /** Exercises
    * 1) Read movies.json as an rdd
    * 2) Show the distinct genres as an RDD
    * 3) Select all the movies in drama genre & IMDB rating > 6
    * 4) Show average rating of movies by genre
    */

  // Exercise 1
  println("Exercise 1")

  case class Movie(title: String, genre: String, rating: Double)

  val moviesRDD = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    )
    .na
    .fill(0, List("rating"))
    .as[Movie]
    .rdd

  moviesRDD.foreach(println)

  // Exercise 2
  println("Exercise 2")

  moviesRDD
    .map(_.genre)
    .distinct()
    .foreach(println)

  // Exercise 3
  println("Exercise 3")

  moviesRDD
    .filter(movie => movie.genre == "Drama" && movie.rating >= 6)
    .foreach(println)

  // Exercise 4
  println("Exercise 4")
  moviesRDD.groupBy(_.genre).map { case (genre, movies) =>
    (
      genre,
      movies
        .map(_.rating)
        .sum / movies.size
    )
  }
    .foreach(println)

  moviesRDD
    .toDF()
    .groupBy("genre")
    .avg("rating")
    .show()
}
