package playground

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object CommonTypesPlayground extends App {

  val spark = SparkSession
    .builder()
    .appName("common-types-playground")
    .master("local[*]")
    .getOrCreate()

  val moviesDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value into DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // boolean
  val goodDramaFilter: Column =
    col("Major_Genre") === "Drama" and col("IMDB_Rating") > 7.0
  moviesDF.where(goodDramaFilter).show()
  // + multiple ways of filtering

  val moviesWithFlagsDF: DataFrame =
    moviesDF.select(col("Title"), goodDramaFilter.as("good_movie"))

  moviesWithFlagsDF.show()

  // filtering on the boolean column name
  moviesWithFlagsDF.filter(col("good_movie")).show()

  // negation
  moviesWithFlagsDF.where(!col("good_movie")).show()

  // Numbers

  // Math operation
  moviesDF
    .select(
      col("Title"),
      ((col("IMDB_Rating") + col("Rotten_Tomatoes_Rating") / 10) / 2)
        .as("Total_Rating")
    )
    .show()

  // corelation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  //Strings
  val carsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("ford")).show()

  // regex
  val regexString = "volkswagen|vw|ford"
  val vwfDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  vwfDF.show()

  // replace with regex
  vwfDF
    .select(
      col("Name"),
      regexp_replace(
        col("Name"),
        regexString,
        "cool car"
      ).as("replaced_regex")
    )
    .where(col("replaced_regex") =!= "")
    //.drop("replaced_regex")
    .show()

  /** Exercise:
    * Filter the cars DF by list of car names obtained by API call
    * Versions:
    *   - contains
    *   - regex
    */

//  def getCarNames(cars: DataFrame): List[String] = for {
//    car: Row <- cars.select("Name").rdd.collect().toList
//    processedCar: String = car.toString()
//  } yield processedCar.substring(1, processedCar.length - 1)

  // regex version
  def getCarNames: List[String] = List("Volkswagen", "Ford", "Audi")
  val complexRegex: String =
    getCarNames.reduce(_ + "|" + _) // or mkString("| ")
  val filteredCars: DataFrame = carsDF
    .select(
      col("Name"),
      regexp_extract(initcap(col("Name")), complexRegex, 0).as("regex_complex")
    )
    .where(col("regex_complex") =!= "")
    .drop("regex_complex")
  filteredCars.show()

  // version 2 - contains
  val carBindedFilters: List[Column] = for {
    car <- getCarNames
  } yield col("Name").contains(car.toLowerCase)

  val filteredCarsWithContains =
    carsDF.filter(carBindedFilters.reduce(_ or _))

  filteredCarsWithContains.show()

}
