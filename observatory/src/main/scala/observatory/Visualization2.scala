package observatory

import java.io.File

import com.sksamuel.scrimage.{Image,           Pixel}
import observatory.Interaction.{generateTiles, multiplier, tileLocation, tilePixels}
import observatory.Visualization.interpolateColor

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 extends Visualization2Interface {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00:   Temperature,
    d01:   Temperature,
    d10:   Temperature,
    d11:   Temperature
  ): Temperature = {
    val x = point.x
    val y = point.y
    (d00 * (1 - x) + d10 * x) * (1 - y) + y * (d01 * (1 - x) + d11 * x)
  }

  def temperaturePredictionHelper(grid: GridLocation ⇒ Temperature, loc: Location): Temperature = {
    val lon      = loc.lon
    val lat      = loc.lat
    val lonCeil  = lon.ceil.toInt
    val lonFloor = lon.floor.toInt
    val latCeil  = lat.ceil.toInt
    val latFloor = lat.floor.toInt
    val x        = lon - lonFloor
    val y        = lat - latFloor
    val t00 = grid(GridLocation(latFloor, lonFloor))
    val t01 = grid(GridLocation(latCeil,  lonFloor))
    val t10 = grid(GridLocation(latFloor, lonCeil))
    val t11 = grid(GridLocation(latCeil,  lonCeil))
    bilinearInterpolation(CellPoint(x, y), t00, t01, t10, t11)
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid:   GridLocation ⇒ Temperature,
    colors: Iterable[(Temperature, Color)],
    tile:   Tile
  ): Image = {
    val pixels = tilePixels(tile)
      .map(tileLocation)
      .map(temperaturePredictionHelper(grid, _))
      .map(interpolateColor(colors, _))
      .map(col ⇒ Pixel(col.red, col.green, col.blue, 127))
      .toArray
    Image(multiplier, multiplier, pixels).scale(256 / multiplier)
  }

  def generateImageDeviation(year: Year, tile: Tile, data: GridLocation ⇒ Temperature): Unit = {
    val colors = List(
      (7d,  Color(0,   0,   0)),
      (4d,  Color(255, 0,   0)),
      (2d,  Color(255, 255, 0)),
      (0d,  Color(255, 255, 255)),
      (-2d, Color(0,   255, 255)),
      (-7d, Color(0,   0,   255))
    )
    val image = this.visualizeGrid(data, colors, tile)
    val path  = s"target/deviations/$year/${tile.zoom}/${tile.x}-${tile.y}.png"
    val file  = new File(path)
    file.getParentFile.mkdirs()
    image.output(file)
  }

  def main(args: Array[String]): Unit = {
    val temperaturess = for {
      year ← 1975 to 1975
    } yield {
      println("Computing data for year: " + year)
      val yearData = Extraction.locateTemperatures(year, "/stations.csv", s"/$year.csv")
      Extraction.locationYearlyAverageRecords(yearData)
    }
    val normals = Manipulation.average(temperaturess)

    val yearlyGrids = for {
      year ← 2015 to 2015
    } yield {
      println("Computing data for year: " + year)
      val yearData = Extraction.locateTemperatures(year, "/stations.csv", s"/$year.csv")
      val avgData  = Extraction.locationYearlyAverageRecords(yearData)
      (year, Manipulation.deviation(avgData, normals))
    }

    Interaction.generateTiles(yearlyGrids, generateImageDeviation)

  }

}
