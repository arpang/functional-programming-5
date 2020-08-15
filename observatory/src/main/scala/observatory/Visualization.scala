package observatory

import com.sksamuel.scrimage.{Image,        Pixel}
import org.apache.spark.sql.functions.{col, udf, _}
import org.apache.spark.sql.{DataFrame,     SparkSession}

import scala.math.Pi

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Extraction")
      .master("local")
      .getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  def isAntipode(loc1: Location, loc2: Location): Boolean =
    loc1.lat + loc2.lat == 0 && math.abs(loc1.lon - loc2.lon) == 180d

  def distance(p: Location, q: Location): Double = {
    val pLat = p.lat * Pi / 180
    val pLon = p.lon * Pi / 180
    val qLat = q.lat * Pi / 180
    val qLon = q.lon * Pi / 180
    val dLan = math.abs(pLat - qLat)
    val dLon = math.abs(pLon - qLon)

    if (dLan == 0 && dLon == 0) 0
    else if (isAntipode(p, q)) Pi
    else math.cosh(math.sin(pLat) * math.sin(qLat) + math.cos(pLat) * math.cos(qLat) * math.cos(dLon))
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature =
    sparkPredictTemperature(
      sc.parallelize(temperatures.map(x ⇒ (x._1.lat, x._1.lon, x._2)).toList).toDF("lat", "lon", "temperature"),
      location
    )

  def sparkPredictTemperature(temperatures: DataFrame, location: Location): Temperature = {
    val weightUDF = udf((lat: Double, lon: Double) ⇒ 1.0 / distance(Location(lat, lon), location))

    val found = temperatures
      .filter(col("lat") === location.lat && col("lon") === location.lon)
      .collect()
      .map(_.getAs[Double]("temperature"))
      .headOption

    found.getOrElse {
      val row = temperatures
        .withColumn("weight", weightUDF(col("lat"), col("lon")))
        .withColumn("weightedTemp", col("weight") * col("temperature"))
        .agg(
          sum(col("weightedTemp")).as("weightedTempSum"),
          sum(col("weight")).as("weightSum")
        )
        .collect()
        .head
      row.getAs[Double]("weightedTempSum") / row.getAs[Double]("weightSum")
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color =
    sparkInterpolateColor(sc.parallelize(points.toList).toDF("temperature", "color"), value)

  def sparkInterpolateColor(points: DataFrame, value: Temperature): Color = {

    val higherOption = points
      .sort(col("temperature"))
      .filter(col("temperature") >= value)
      .head(1)
      .headOption
      .map { row ⇒
        (row.getAs[Double]("temperature"), row.getAs[Color]("color"))
      }

    val lowerOption = points
      .sort(col("temperature").desc)
      .filter(col("temperature") <= value)
      .head(1)
      .headOption
      .map { row ⇒
        (row.getAs[Double]("temperature"), row.getAs[Color]("color"))
      }

    if (higherOption.isEmpty && lowerOption.isEmpty) Color(0, 0, 0)
    else if (higherOption.isEmpty) lowerOption.get._2
    else if (lowerOption.isEmpty) higherOption.get._2
    else {
      val (t1, c1) = higherOption.get
      val (t0, c0) = lowerOption.get
      val (r1, g1, b1) = (c1.red, c1.green, c1.blue)
      val (r0, g0, b0) = (c0.red, c0.green, c0.blue)
      val r = ((r0 * (t1 - value) + r1 * (value - t0)) / (t1 - t0)).toInt
      val g = ((g0 * (t1 - value) + g1 * (value - t0)) / (t1 - t0)).toInt
      val b = ((b0 * (t1 - value) + b1 * (value - t0)) / (t1 - t0)).toInt
      Color(r, g, b)
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    sparkVisualize(
      sc.parallelize(temperatures.map(x ⇒ (x._1.lat, x._1.lon, x._2)).toList).toDF("lat", "lon", "temperature"),
      sc.parallelize(colors.toList).toDF("temperature", "color")
    )
  }

  def sparkVisualize(temperatures: DataFrame, colors: DataFrame): Image = {
    val temperaturesCached = temperatures.persist()
    val colorsCached       = colors.persist()
    val pixels = (for {
      lon ← -180 to 180
      lat ← 90 to (-90, -1)
    } yield {
      val temp  = sparkPredictTemperature(temperaturesCached, Location(lat, lon))
      val color = sparkInterpolateColor(colorsCached,         temp)
      Pixel(color.red, color.green, color.green, 1)
    }).toArray

    Image(360, 180, pixels)
  }

}
