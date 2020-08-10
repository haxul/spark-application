package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String): List[StockValue] = Source
    .fromFile(filename)
    .getLines()
    .drop(1)
    .map(line => line.split(","))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))
  val stockRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")
  import spark.implicits._
  val stockRDD3 = stocksDF.as[StockValue].rdd

  val numbersDF = numbersRDD.toDF("numbers")
  val numbersDS = spark.createDataset(numbersRDD)


  val msftRDD = stocksRDD.filter(_.symbol == "MSFT")
  val msftCount = msftRDD.count()
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct()


}

/*
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+

  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
 */