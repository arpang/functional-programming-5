package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction extends ExtractionInterface {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Extraction")
      .master("local")
      .getOrCreate()
  import spark.implicits._
  val sc   = spark.sparkContext
  val fToC = udf((f: Double) ⇒ (f - 32.0) / 1.8)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(
    year:             Year,
    stationsFile:     String,
    temperaturesFile: String
  ): Iterable[(LocalDate, Location, Temperature)] = {
    sparkLocateTemperatures(year, stationsFile, temperaturesFile).collect().map { row ⇒
      val month       = row.getAs[Int]("month")
      val day         = row.getAs[Int]("day")
      val latitude    = row.getAs[Double]("latitude")
      val longitude   = row.getAs[Double]("longitude")
      val temperature = row.getAs[Double]("celsius")
      (LocalDate.of(year, month, day), Location(latitude, longitude), temperature)
    }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(
    records: Iterable[(LocalDate, Location, Temperature)]
  ): Iterable[(Location, Temperature)] = {
    val modified = records.map { case (date, location, temp) ⇒ (location, temp) }
    sparkLocationYearlyAverageRecords(sc.parallelize(modified.toList).toDS()).collect()
  }

  def sparkLocateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): DataFrame = {

    val station =
      Source
        .fromInputStream(getClass.getResourceAsStream(stationsFile), "utf-8")
        .getLines()
        .map(_.split(","))
        .filter(_.length == 4)
        .map(x ⇒ (x(0), x(1), x(2).toDouble, x(3).toDouble))
        .toList
        .toDF("stn", "wban", "latitude", "longitude")

    val temperature =
      Source
        .fromInputStream(getClass.getResourceAsStream(temperaturesFile), "utf-8")
        .getLines()
        .map(_.split(","))
        .filter(_.length == 5)
        .map(x ⇒ (x(0), x(1), x(2).toInt, x(3).toInt, x(4).toDouble))
        .toList
        .toDF("stn", "wban", "month", "day", "temperature")

    val stationClean     = station.na.drop()
    val temperatureClean = temperature.na.drop().filter($"temperature" =!= 9999.9d)
    temperatureClean.join(stationClean, Seq("stn", "wban")).withColumn("celsius", fToC(temperatureClean("temperature")))
  }

  def sparkLocationYearlyAverageRecords(records: Dataset[(Location, Temperature)]): Dataset[(Location, Temperature)] = {
    records
      .groupByKey(x ⇒ x._1)
      .agg(
        typed.avg(_._2)
      )
      .persist()
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val first  = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")
    val second = Extraction.locationYearlyAverageRecords(first)
//    println(Visualization.predictTemperature(second, Location(5D, 5D)))
//    println(second.head._1, second.head._2)
//    println(Visualization.visualize(second, Location(90, -180)))
    val list = List(
      (60d,  Color(255, 255, 255)),
      (32d,  Color(255, 0,   0)),
      (12d,  Color(255, 255, 0)),
      (0d,   Color(0,   255, 255)),
      (-15d, Color(0,   0,   255)),
      (-27d, Color(255, 0,   255)),
      (-50d, Color(33,  0,   107)),
      (-60d, Color(0,   0,   0))
    )
    val third = Visualization.visualize(second, list)
    println(third.metadata)
//    println(Visualization.interpolateColor(List((0.0,Color(255,0,0)), (1.52587890625E-5,Color(0,0,255))), 0.0)
  }
}
