package part3typesdatasets

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val path = "src/main/resources/data"
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json(s"$path/movies.json")

  moviesDF.select(col("Title"), lit(47).as("plain_value"))
  val dramFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0

  moviesDF.select("Title").where(dramFilter and goodRatingFilter)
  val moviesWithGoodFlagDF  = moviesDF.select(col("Title"), (dramFilter and goodRatingFilter).as("is_good_film"))
  moviesWithGoodFlagDF.where("is_good_film") // where col("is_good_film") === true
  moviesWithGoodFlagDF.where(not(col("is_good_film"))) // where col("is_good_film") =!= true

  moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating") / 2))

  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$path/cars.json")

  carsDF.select(initcap(col("Name")))
  carsDF.select("*").where(col("Name").contains("volkswagen"))
  val regex = "volkswagen|vw"
  val wvCarDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")


  wvCarDF.select(
    col("Name"),
    regexp_replace(col("Name"), regex, "People's car").as("replace")
  )


  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val reg = getCarNames.map(_.toLowerCase).mkString("|")

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), reg, 0).as("match")
  ).where(col("match") =!= "").drop(col("match"))

  val carNamesFilterList = getCarNames.map(_.toLowerCase).map(name => col("Name").contains(name))
  val bigFilter = carNamesFilterList.fold(lit(false)) ((f1, f2) => f1 or f2)

  carsDF.select("*").where(bigFilter)
}
