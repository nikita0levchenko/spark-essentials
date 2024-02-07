package playground

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object JoinsPlayground extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Joins playground")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bandsDf: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitarsDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  // joins

  // inner joins
  val joinCondition: Column = bandsDf.col("id") === guitarPlayersDF.col("band")
  val preparedGuitarsPlayers: DataFrame =
    guitarPlayersDF.withColumnRenamed("id", "band_id")
  val bandsWithMembersInfoIJDf: DataFrame = bandsDf
    .join(
      preparedGuitarsPlayers,
      joinCondition,
      "inner"
    )
    .drop("band")
    .withColumnRenamed("id", "band_id")

  bandsWithMembersInfoIJDf.printSchema()
  bandsWithMembersInfoIJDf.show()

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with null where the data is missing
  val bandsWithMembersInfoLeftOuterDF: DataFrame = bandsDf
    .join(
      preparedGuitarsPlayers,
      joinCondition,
      "left_outer"
    )
    .drop("band")
    .withColumnRenamed("id", "band_id")

  bandsWithMembersInfoLeftOuterDF.printSchema()
  bandsWithMembersInfoLeftOuterDF.show()

  // right outer join = everything in the inner join + all the rows in the RIGHT table, with null where the data is missing
  val bandsWithMembersInfoRightOuterDF: DataFrame = bandsDf
    .join(
      preparedGuitarsPlayers,
      joinCondition,
      "right_outer"
    )
    .drop("band")
    .withColumnRenamed("id", "band_id")

  bandsWithMembersInfoRightOuterDF.printSchema()
  bandsWithMembersInfoRightOuterDF.show()

  // outer join = everything in the inner join + all the rows in the BOTH table, with null where the data is missing
  val bandsWithMembersInfoOuterDF: DataFrame = bandsDf
    .join(
      preparedGuitarsPlayers,
      joinCondition,
      "outer"
    )
    .drop("band")
    .withColumnRenamed("id", "band_id")

  bandsWithMembersInfoOuterDF.printSchema()
  bandsWithMembersInfoOuterDF.show()

  // semi-join
  // semi_left join = data only from LEFT table, which satisfying join condition
  val bandsWithGuitarPlayersInfoLeftSemiDF: DataFrame = bandsDf
    .join(
      preparedGuitarsPlayers,
      joinCondition,
      "left_semi"
    )
    .drop("band")
    .withColumnRenamed("id", "band_id")

  bandsWithGuitarPlayersInfoLeftSemiDF.printSchema()
  bandsWithGuitarPlayersInfoLeftSemiDF.show()

  // anti-join = all rows from LEFT table which NOT satisfying join condition
  val bandsWithGuitarPlayersInfoRightSemiDF: DataFrame = bandsDf
    .join(
      preparedGuitarsPlayers,
      joinCondition,
      "left_anti"
    )
    .drop("band")
    .withColumnRenamed("id", "band_id")

  bandsWithGuitarPlayersInfoRightSemiDF.printSchema()
  bandsWithGuitarPlayersInfoRightSemiDF.show()

  // things to bear in mind
  /* val res = bandsDf.join(guitarPlayersDF, bandsDf.col("id") === guitarPlayersDF.col("band"))
  res.select("id", "band") - will throw exception because res contains two 'id' columns
   */

  // How we can solve this?
  // Option 1 - we can rename the column on which we are join
  val resultDF: DataFrame = guitarPlayersDF.join(
    bandsDf.withColumnRenamed("id", "band"),
    "band"
  )

  // Option 2 = drop the dup columns
  val result2DF = guitarPlayersDF
    .join(
      bandsDf,
      guitarPlayersDF("band") === bandsDf("id"),
      "inner"
    )
    .drop(bandsDf.col("id"))

  // Option 3 - rename offending column
  val bandsModDF = bandsDf.withColumnRenamed("id", "band_id")

  val result3DF = guitarPlayersDF.join(
    bandsModDF,
    guitarPlayersDF("band") === bandsModDF("band_id")
  )

  // using complex data
  val guitarWithGuitaristsDF = guitarPlayersDF.join(
    guitarsDF.withColumnRenamed("id", "guitar_id"),
    expr("array_contains(guitars, guitar_id )")
  )

  /** Exercises:
    * 1) Show all employees and their max salaries
    * 2) Show all employees who were never manager
    * 3) Find the job titles of the best paid 10 employees in the company
    */

  //Exercise #1

  val rtjvmConfig: Config = Config(
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/rtjvm",
    "docker",
    "docker"
  )
  def readTable(tableName: String, config: Config): DataFrame = spark.read
    .format("jdbc")
    .option("driver", config.driver)
    .option("url", config.url)
    .option("user", config.user)
    .option("password", config.password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF: DataFrame = readTable("employees", rtjvmConfig)
  employeesDF.printSchema()
  employeesDF.show()

  val salariesDF: DataFrame = readTable("salaries", rtjvmConfig)
  salariesDF.printSchema()
  salariesDF.show()

  val maxSalariesDF: DataFrame =
    salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesWithSalariesDF = employeesDF
    .join(maxSalariesDF, "emp_no")
    .drop(salariesDF.col("emp_no"))
  println("EXERCISE #1")
  employeesWithSalariesDF.printSchema()
  employeesWithSalariesDF.show()

  // Exercise 2
  val deptManagerDF = readTable("dept_manager", rtjvmConfig)

  val employeesNoWereManagerDF = employeesDF
    .join(
      deptManagerDF,
      employeesDF("emp_no") === deptManagerDF("emp_no"),
      "left_anti"
    )
    .drop(deptManagerDF("emp_no"))
  println("EXERCISE #2")
  employeesNoWereManagerDF.printSchema()
  employeesNoWereManagerDF.show()

  // Exercise 3
  val titlesDF = readTable("titles", rtjvmConfig)

  val mostRecentsTitlesDF: DataFrame =
    titlesDF.groupBy("emp_no", "title").agg(max("to_date").as("max_to_date"))

  val top10PaidEmployees =
    employeesWithSalariesDF.orderBy(col("maxSalary").desc).limit(10)

  val jobTitlesOfTheBestPaiedEmployeesDF =
    top10PaidEmployees.join(mostRecentsTitlesDF, "emp_no")

  println("EXERCISE #3")
  jobTitlesOfTheBestPaiedEmployeesDF.printSchema()
  jobTitlesOfTheBestPaiedEmployeesDF.show()
}

case class Config(driver: String, url: String, user: String, password: String)
