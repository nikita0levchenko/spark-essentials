package playground

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ColumnsAndExpressionsPlayground extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Columns_and_expressions_playground")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val carsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  carsDF.show()

  // Columns
  val firstColumn: Column = carsDF
    .col("Name")

  //Selecting a few columns(Projection technics
  val carNamesDF: DataFrame = carsDF.select(firstColumn)
  carNamesDF.show()
  carNamesDF.printSchema()

  // various select methods
  import spark.implicits._
   val namesAndOriginsDF = carsDF.select("Name","Origin")
  namesAndOriginsDF.printSchema()
  namesAndOriginsDF.show()

  //EXPRESSIONS
  val simplestExpression: Column = carsDF.col("Weight_in_lbs")
  val weightInKg: Column = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeightsDf = carsDF.select(
    col("Name"),
    weightInKg.as("Weight_in_Kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_lbs_expr")
  )
  carsWithWeightsDf.printSchema()
  carsWithWeightsDf.show()

  val carsWithSelectExprDF = carsDF
    .selectExpr("Name", "Weight_in_lbs / 2.2")
  carsWithSelectExprDF.printSchema()
  carsWithSelectExprDF.show()

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)
  carsWithKg3DF.printSchema()
  carsWithKg3DF.show()

  val dropedCarsWithKg3DF = carsWithKg3DF.drop("Cylinders", "Displacement")
  dropedCarsWithKg3DF.printSchema()
  dropedCarsWithKg3DF.show()

  //Filtering
  val carsNotFromUSADF = carsDF.filter(col("Origin") =!= "USA")
  carsNotFromUSADF.printSchema()
  carsNotFromUSADF.show()

  val carsNotFromUSADF2 = carsDF.where(col("Horsepower") > 200)
  carsNotFromUSADF2.show()

  //chain filters
  val americanPowerfulCarsDF = carsDF
    .filter(col("Origin") === "USA" and col("Horsepower") > 200)
  americanPowerfulCarsDF.show()

  // unioning = adding more rows
   val moreCarsDf = spark.read
     .option("inferSchema", "true")
     .json("src/main/resources/data/more_cars.json")

  moreCarsDf.show()

  val allCars = carsDF union moreCarsDf // work if DFs have the same schema
  allCars.show()

  val allCountriesDf = allCars.select("Origin").distinct()
  allCountriesDf.show()

  /*
  1) Create moviesDf from movies.json and choose 2 columns of your choise
  2) Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + US_DVD_Sales
  3) Select all COMEDY movies with IMDB rating above 6
   */

  println("/-------------------Exercises-------------------/")

  println("First exercise:")
  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesSelectedDF = moviesDf.select("Major_Genre", "Title")

  moviesSelectedDF.printSchema()
  moviesSelectedDF.show()

  println("Second exercise")
  val moviesWithGeneralProfitDf = moviesDf
    .withColumn("General_profit", col("US_Gross") + col("Worldwide_Gross"))

  moviesWithGeneralProfitDf.printSchema()
  moviesWithGeneralProfitDf.show()

  println("Third exercise")
  val bestComedies = moviesDf.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  bestComedies.printSchema()
  bestComedies.show()
}
