package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Agg")
    .config("spark.master", "local")
    .getOrCreate()
  val path = "src/main/resources/data"
  val moviesDF = spark.read.option("inferSchema", "true").json(s"$path/movies.json")


  val genreCount = moviesDF.select(count(col("Major_Genre"))) //all values except null
  moviesDF.select(count("*"))// all includes null
  moviesDF.select(countDistinct(col("Major_Genre")))
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.select(avg(col("IMDB_Rating")))
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  moviesDF.groupBy(col("Major_Genre")).count().show()


}
