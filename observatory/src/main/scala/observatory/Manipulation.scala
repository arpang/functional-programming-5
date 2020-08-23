package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation extends ManipulationInterface {

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val preComputed = (for {
      lat ← (90 until (-90, -1)).par
      lon ← (-180 until 180).par
    } yield {
      val gridLocation = GridLocation(lat, lon)
      val location = Location(lat.toDouble, lon.toDouble)
      gridLocation → Visualization.predictTemperature(temperatures, location)
    }).toMap.seq

    gridLocation: GridLocation ⇒ preComputed(gridLocation)
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val num = temperaturess.size
    val grids = for {
      temperatures ← temperaturess.par
    } yield makeGrid(temperatures)

    val preComputed = (for {
      lat ← (90 until (-90, -1)).par
      lon ← (-180 until 180).par
    } yield {
      val gridLocation = GridLocation(lat, lon)
      val avgTemp = grids.map(grid ⇒ grid(gridLocation)).sum / num
      gridLocation → avgTemp
    }).toMap.seq

    gridLocation: GridLocation ⇒ preComputed(gridLocation)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = makeGrid(temperatures)

    val preComputed = (for {
      lat ← (90 until (-90, -1)).par
      lon ← (-180 until 180).par
    } yield {
      val gridLocation = GridLocation(lat, lon)
      val curValue = grid(gridLocation)
      val normalValue = normals(gridLocation)
      gridLocation → (curValue - normalValue)
    }).toMap.seq

    gridLocation: GridLocation ⇒ preComputed(gridLocation)
  }


}

