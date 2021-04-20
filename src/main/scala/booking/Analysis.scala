package booking

import java.time.{LocalDate, ZoneId}

import booking.analysis.Report
import booking.analysis.input.Bookings.eligibleForAnalysis
import booking.analysis.input.{Airports, Bookings}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analysis {

  val logger = Logger.getLogger(Analysis.getClass)

  //    val base = s"${System.getProperty("user.home")}/dev/assignment-booking-analysis/data"
  //    val bookingsPath = s"$base/bookings/booking.json"
  //    val airportsFile = s"$base/airports/airports.dat"
  //    val startUtc = "2010-02-05T13:05:00Z"
  //    val endUtc = "2025-02-05T13:05:00Z"

  def main(args: Array[String]): Unit = {
    val Array(start, end, bookingsPath, airportsFile) = args
    require(!start.isEmpty && !end.isEmpty, "Start and end dates should not be empty")
    val datePattern = "^\\d{4}-\\d{2}-\\d{2}$"
    require(start.matches(datePattern) && end.matches(datePattern), "Start and end date format should be yyyy-MM-dd")
    require(!bookingsPath.isEmpty, "Bookings data path should be provided")
    require(!airportsFile.isEmpty, "Airports data file path should be provided")

    val startUtc: String = LocalDate.parse(start).atStartOfDay(ZoneId.of("UTC")).toInstant().toString
    val endUtc: String = LocalDate.parse(end).atTime(23, 59, 59).atZone(ZoneId.of("UTC")).toString

    val spark = SparkSession.builder
      .appName("booking-analysis")
      .config(new SparkConf())
//      .master("local[4]") //use to run from IDE
      .getOrCreate()
    logger.info("Starting Analysis")

    val airports = Airports.toLookup(spark, airportsFile)
    val airportsToCountry = airports.map(m => (m._1, m._2._1))
    val airportsToTimezone = airports.map(m => (m._1, m._2._2))
    val broadcastAirportsToCountry = spark.sparkContext.broadcast(airportsToCountry)
    val broadcastAirportsToTimezone = spark.sparkContext.broadcast(airportsToTimezone)

    val bookings = Bookings.load(spark, bookingsPath)
    val eligibleBookings = eligibleForAnalysis(bookings, startUtc, endUtc, broadcastAirportsToCountry.value)
    val report = Report.run(spark, eligibleBookings, broadcastAirportsToCountry, broadcastAirportsToTimezone)
    report.show(report.count().toInt, false)
    spark.stop()
  }
}
