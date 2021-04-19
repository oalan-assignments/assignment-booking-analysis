package booking.analysis.input

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Airports {

  case class LocationInfo(iata: String, country: String, tz: String)

  def toLookup(spark: SparkSession, path: String): Map[String, (String, String)] = {
    import spark.implicits._
    spark.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(path)
      .toDF("id"
        , "name"
        , "city"
        , "country"
        , "iata"
        , "icao"
        , "lat"
        , "lon"
        , "alt"
        , "timezone"
        , "dst"
        , "tz"
        , "airportType"
        , "source")
      .select("iata", "country", "tz")
      .filter(col("iata") =!= "\\N")
      .filter(col("tz") =!= "\\N")
      .as[LocationInfo]
      .collect()
      .map(a => a.iata -> (a.country, a.tz))
      .toMap
  }
}
