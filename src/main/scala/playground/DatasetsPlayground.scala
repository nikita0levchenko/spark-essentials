package playground

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.avg

object DatasetsPlayground extends App {
  val spark = SparkSession
    .builder()
    .appName("datasets-playground")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val numbersDS: Dataset[Int] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")
    .as[Int] // this is how we convert DataFrame into DataSet

  def readDataFrameFromJSON(fileName: String): DataFrame =
    spark.read.json(fileName)

  // dataset of a complex type
  // 1 - define your type(case class)
  // 2 - read DF from file
  // 3 - import the implicit Encoder or create him by yourself
  // 4 - convert DF to DS

  val carsDS = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .json("src/main/resources/data/cars.json")
    .as[Car]

  // DS collections functions
  numbersDS
    .filter(_ > 200000)
    .map(_ / 1000)

  // map, flatMap, reduce, filter, for-comprehensions...
  carsDS
    .map(_.Name.toUpperCase())

  /** Exercises
    * 1) Count how many cars do we have
    * 2) How many powerful cars(Horsepower > 140)
    * 3) Average horsepower for the entire dataset
    */

  // Exercise 1
  val countOfCar: Long = carsDS.count()
  //println(countOfCar)

  // Exercise 2
  val powerfulCarsCount: Long = carsDS
    .filter(car => car.Horsepower.getOrElse(0L) > 140)
    .count()
  //println(powerfulCarsCount)

  // Exercise 3
  val averageHorsepower: Double = carsDS
    .map(_.Horsepower.getOrElse(0L))
    .reduce(_ + _)
    .toDouble / carsDS.count()
  //println(averageHorsepower)

  carsDS
    .select(avg("Horsepower"))
  //.show()

  val guitarsDS = spark.read
    .json("src/main/resources/data/guitars.json")
    .as[Guitar]

  val guitarPlayersDS = spark.read
    .json("src/main/resources/data/guitarPlayers.json")
    .as[GuitarPlayer]

  val bandDS = spark.read
    .json("src/main/resources/data/bands.json")
    .as[Band]

  val guitarPlayersBandDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(
      bandDS,
      guitarPlayersDS.col("band") === bandDS.col("id"),
      "inner"
    )

  guitarPlayersBandDS.printSchema()
  guitarPlayersBandDS.show()

  // Exercise: join the guitarPlayersDS with guitarDS whet guitar id contains in guitars

  val guitarPlayersWithGuitars = guitarPlayersDS
    .joinWith(
      guitarsDS,
      array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
      "outer"
    )

  guitarPlayersWithGuitars.printSchema()
  guitarPlayersWithGuitars.show()

  // grouping data set
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()

  carsGroupedByOrigin.printSchema()
  carsGroupedByOrigin.show()

  // Joins and groups are involve shuffle operations, because they are WIDE transformations
}

// Joins
case class Guitar(id: Long, make: String, model: String, guitarType: String)
case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
case class Band(id: Long, name: String, hometown: String, year: Long)

case class Car(
    Name: String,
    Miles_per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: String,
    Origin: String
)
