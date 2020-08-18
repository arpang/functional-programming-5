package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.math.Pi

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends VisualizationInterface {

  val r = 6378 // radius of earth
  val pInverseWeight = 6

  def areAntipodes(loc1: Location, loc2: Location): Boolean =
    loc1.lat + loc2.lat == 0 && math.abs(loc1.lon - loc2.lon) == 180d

  def distance(p: Location, q: Location): Double = {
    val pLat = p.lat * Pi / 180
    val pLon = p.lon * Pi / 180
    val qLat = q.lat * Pi / 180
    val qLon = q.lon * Pi / 180
    val dLan = math.abs(pLat - qLat)
    val dLon = math.abs(pLon - qLon)

    if (dLan == 0 && dLon == 0) 0
    else if (areAntipodes(p, q)) Pi
    else math.acos(math.sin(pLat) * math.sin(qLat) + math.cos(pLat) * math.cos(qLat) * math.cos(dLon))
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val found = temperatures.find(x ⇒ (r * distance(location, x._1)) <= 1).map(_._2)

    found.getOrElse {
      val (weightedTempSum, weightSum) = temperatures
        .map {
          case (point, temperature) ⇒
            val weight = 1.0 / math.pow(distance(point, location), pInverseWeight)
            (temperature * weight, weight)
        }
        .reduce[(Double, Double)] {
          case (a: (Double, Double), b: (Double, Double)) ⇒ (a._1 + b._1, a._2 + b._2)
        }
      weightedTempSum / weightSum
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val sorted       = points.toList.sortBy(_._1)
    val higherOption = sorted.find(_._1 >= value)
    val lowerOption  = sorted.reverse.find(_._1 <= value)

    if (higherOption.isEmpty && lowerOption.isEmpty) Color(0, 0, 0)
    else if (higherOption.isEmpty) lowerOption.get._2
    else if (lowerOption.isEmpty) higherOption.get._2
    else {
      val (t1, c1) = higherOption.get
      val (t0, c0) = lowerOption.get
      if (t1 == t0) c1
      else {
        val (r1, g1, b1) = (c1.red, c1.green, c1.blue)
        val (r0, g0, b0) = (c0.red, c0.green, c0.blue)
        val r = ((r0 * (t1 - value) + r1 * (value - t0)) / (t1 - t0)).round.toInt
        val g = ((g0 * (t1 - value) + g1 * (value - t0)) / (t1 - t0)).round.toInt
        val b = ((b0 * (t1 - value) + b1 * (value - t0)) / (t1 - t0)).round.toInt
        Color(r, g, b)
      }
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val cords = for {
      lat ← 90 until (-90, -1)
      lon ← -180 until 180
    } yield (lat, lon)
    val pixels = cords.par.map { case (lat, lon) ⇒
      val temp  = predictTemperature(temperatures, Location(lat, lon))
      val color = interpolateColor(colors,         temp)
      Pixel(color.red, color.green, color.blue, 127)
    }.toArray
    Image(360, 180, pixels)
  }

}
