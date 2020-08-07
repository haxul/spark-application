package part3typesdatasets

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}


object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dateset of the complex type
  // 1 - define case class
  case class Car(
                  name: String,
                  miles_per_Gallon: Option[Double],
                  cylinders: Long,
                  displacement: Double,
                  horsepower: Long,
                  weight_in_lbs: Long,
                  acceleration: Double,
                  year: String,
                  origin: String
                )

  // 2 - read file from the file
  def readDF(filename: String) = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")
  // 3 - define Encoder

  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  case class CarWithNameAndHorsepower(Name: String, Horsepower: Long)
  carsDS.map(car => CarWithNameAndHorsepower(car.name, car.horsepower))

}
