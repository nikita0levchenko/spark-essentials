package playground

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataSources.stocksSchema

object DataSourcesPlayground extends App {
  val spark = SparkSession.builder()
    .appName("data-sources-app")
    .master("local[*]")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

   val carsDF: DataFrame = spark.read
     .schema(carsSchema)
     .json("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/cars.json")

  //JSON flags
  val carsDFWithJSONFlags = spark.read
    .schema(carsSchema)
    .options(
      Map(
        "dateFormat" -> "YYYY-MM-dd",
        "compress" -> "bzip",
        "allowSingleQuotes" -> "true"
      )
    )
    .json("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/cars.json")

  carsDFWithJSONFlags.printSchema()
  carsDFWithJSONFlags.show()

  //CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", StringType),
    StructField("price", DoubleType)
  ))

  val stocksDF = spark.read
    .schema(stocksSchema)
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.printSchema()
  stocksDF.show()

  //Parquet
  // spark saving files py default in parquet format

  //Text files
  val textDF = spark.read
    .text("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/sampleTextFile.txt")
  textDF.printSchema()
  textDF.show()

  // Read from remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val jdbcEmployeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  jdbcEmployeesDF.printSchema()
  jdbcEmployeesDF.show()

  //Exercise:
  val moviesDF = spark.read
    .json("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/movies.json")

  moviesDF.write
    .option("header", "true")
    .option("delimiter", "\t")
    .mode(SaveMode.Overwrite)
    .csv("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/separated_movies.csv")

  moviesDF.write
    .option("compression", "snappy")
    .mode(SaveMode.Overwrite)
    .parquet("/Users/Nikita_Levchenko/IdeaProjects/Personal/spark-essentials/src/main/resources/data/snappy_parquet_movies")

  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .mode(SaveMode.Overwrite)
    .save()

  val moviesFromDB = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .load()

  moviesFromDB.printSchema()
  moviesFromDB.show()
}
