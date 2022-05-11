package playground

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataFrameBasicsPlayground extends App {

  val spark = SparkSession
    .builder().
    appName("Data frame basics playground").
    config("spark.master", "local").
    getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val firstDF = spark.read
    .option("inferSchema", "true")
    .json("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/cars.json")

  firstDF.printSchema()
  firstDF.show()
  firstDF.take(10).foreach(println)

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  println(firstDF.schema)

  val secondDf = spark.read
    .schema(carsSchema)
    .json("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/cars.json")

  secondDf.printSchema()
  secondDf.show()

  val smartphoneSchema = StructType(Array(
    StructField("make", StringType),
    StructField("model", StringType),
    StructField("screen_dimension", StructType(Array(
      StructField("height", IntegerType),
      StructField("width", IntegerType)
    ))),
    StructField("camera_megapixels", IntegerType)
  ))

  val smartphones = Seq(
    ("Apple", "Iphone 7", Array(1024, 768), 1000),
    ("Samsung", "Google-pixel", Array(2000, 1000), 2000),
    ("Nolia", "Stone", Array(800, 640), 1500),
  )

  import spark.implicits._
  val smartphonesDF = smartphones.toDF("make", "model", "screen_dimension", "camera_megapixels")
  smartphonesDF.printSchema()
  smartphonesDF.show()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("/Users/Nikita_Levchenko/IdeaProjects/spark-essentials/src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())

}
