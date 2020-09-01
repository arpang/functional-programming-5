package observatory

import java.io.File

import com.sksamuel.scrimage.{Image, Pixel}

import math._
import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import Visualization._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {
  val power = 7
  val multiplier = math.pow(2, power).toInt

  def tilePixels(tile: Tile): ParSeq[Tile] = {
    val cords = for {
      j ← multiplier * tile.y until multiplier * (tile.y + 1)
      i ← multiplier * tile.x until multiplier * (tile.x + 1)
    } yield (i, j)

    cords.par.map {
      case (i, j) ⇒
        Tile(i, j, tile.zoom + power)
    }
  }

  def zoom(tile: Tile): List[Tile] = {
    val cords = for {
      j ← 2 * tile.y until 2 * (tile.y + 1)
      i ← 2 * tile.x until 2 * (tile.x + 1)
    } yield (i, j)

    cords.map {
      case (i, j) ⇒
        Tile(i, j, tile.zoom + 1)
    }.toList
  }

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val lon = (tile.x * 360) / pow(2, tile.zoom) - 180
    val lat = atan(sinh(Pi - (2 * Pi * tile.y) / pow(2, tile.zoom))) * 180 / Pi
    Location(lat, lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(
    temperatures: Iterable[(Location, Temperature)],
    colors:       Iterable[(Temperature, Color)],
    tile:         Tile
  ): Image = {
    val pixels = tilePixels(tile)
      .map(tileLocation)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col ⇒ Pixel(col.red, col.green, col.blue, 127))
      .toArray
    Image(multiplier, multiplier, pixels).scale(256/multiplier)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData:    Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) ⇒ Unit
  ): Unit = {
    val zero   = Tile(0, 0, 0)
    val first  = zoom(zero)
    val second = first.flatMap(zoom)
    val third  = second.flatMap(zoom)
    val all    = zero :: first ++ second ++ third
    for {
      tile ← all.par
      (year, data) ← yearlyData.par
    } generateImage(year, tile, data)
  }

  def generateImageImplementation(year: Year, tile: Tile, data: Iterable[(Location, Temperature)]): Unit = {
    val colors: List[(Temperature, Color)] = List(
      (60d,  Color(255, 255, 255)),
      (32d,  Color(255, 0,   0)),
      (12d,  Color(255, 255, 0)),
      (0d,   Color(0,   255, 255)),
      (-15d, Color(0,   0,   255)),
      (-27d, Color(255, 0,   255)),
      (-50d, Color(33,  0,   107)),
      (-60d, Color(0,   0,   0))
    )
    val image = this.tile(data, colors, tile)
    val path = s"target/temperatures/$year/${tile.zoom}/${tile.x}-${tile.y}.png"
    val file = new File(path)
    file.getParentFile.mkdirs()
    image.output(file)
  }

  def main(args: Array[String]): Unit = {
    val years = (1975 to 2015).par
    years.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(2))

    val yearlyData = for {
      year ← years
    } yield {
      println("Computing data for year: " + year)
      val first  = Extraction.locateTemperatures(year, "/stations.csv", s"/$year.csv")
      (year, Extraction.locationYearlyAverageRecords(first))
    }
    generateTiles(yearlyData.seq, generateImageImplementation)
    println("Done")
  }
}



