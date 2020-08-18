package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import math._
import scala.collection.parallel.ParSeq
import Visualization._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends InteractionInterface {

  def locationToIndex(loc: Location): (Int, Int) = {
    val lat = loc.lat
    val lon = loc.lon

    val i = (lon + 180).round.toInt
    val j = (90 - lat).round.toInt
    (i, j)
  }

  def tilePixels(tile: Tile): ParSeq[Tile] = {
    val cords = for {
      j ← 256 * tile.y until 256 * (tile.y + 1)
      i ← 256 * tile.x until 256 * (tile.x + 1)
    } yield (i, j)

    cords.par.map { case (i, j) ⇒
      Tile(i, j, tile.zoom + 8)
    }
  }

  def zoom(tile: Tile): List[Tile] = {
    val cords = for {
      j ← 2 * tile.y until 2 * (tile.y + 1)
      i ← 2 * tile.x until 2 * (tile.x + 1)
    } yield (i, j)

    cords.map { case (i, j) ⇒
      Tile(i, j, tile.zoom + 1)
    }.toList
  }

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val lon = (tile.x * 360)/pow(2, tile.zoom) - 180
    val lat = atan(sinh(Pi - (2 * Pi * tile.y)/pow(2, tile.zoom))) * 180/Pi
    Location(lat, lon)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val pixels = tilePixels(tile).map(tileLocation)
      .map(predictTemperature(temperatures, _))
      .map(interpolateColor(colors, _))
      .map(col => Pixel(col.red, col.green, col.blue, 127))
      .toArray
    Image(256, 256, pixels)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    val zero = Tile(0, 0, 0)
    val first = zoom(zero)
    val second = first.flatMap(zoom)
    val third = second.flatMap(zoom)
    val all = zero :: first ++ second ++ third
    for {
      tile ← all
      (year, data) ← yearlyData
    } generateImage(year, tile, data)

  }

}
