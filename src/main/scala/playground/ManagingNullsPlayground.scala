package playground

import org.apache.spark.sql.functions.{coalesce, col, column}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ManagingNullsPlayground extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("managing_nulls")
    .master("local[*]")
    .getOrCreate()

  val moviesDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // select first not-null value
  moviesDF
    .select(
      col("Title"),
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
    )
    .show()

  // use a condition to filter null rows
  moviesDF.select(col("*"))
    .where(column("IMDB_Rating").isNotNull)
    .show()

  // nulls when ordering
  moviesDF.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last)
    .select("Title")
    .show()

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating")
    .na.drop() // return new DF, without null or NaN

  // replacing nulls
  moviesDF.select("Title", "IMDB_Rating")
    .na.fill(0, List("IMDB_Rating"))
    .show()

  moviesDF.select("*")
    .na.fill(Map("IMDB_Rating" -> 0, "Rotten_Tomatoes_Rating" -> 10))
    .show()

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // the same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // the same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // return null if first value EQUALS to the second else return first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if(first != null) second else third
  ).show()
}
