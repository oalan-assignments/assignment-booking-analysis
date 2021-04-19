package booking

import java.time.{LocalDate, ZoneId}

import booking.analysis.{Airports, Bookings}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analysis {

  val logger = Logger.getLogger(Analysis.getClass)

  //    val base = s"${System.getProperty("user.home")}/dev/assignment-booking-analysis/data"
  //    val bookingsFile = s"$base/bookings/booking.json"
  //    val airportsFile = s"$base/airports/airports.dat"
  //    val startUtc = "2010-02-05T13:05:00Z"
  //    val endUtc = "2025-02-05T13:05:00Z"

  def main(args: Array[String]): Unit = {
    val Array(start, end, bookingsFile, airportsFile) = args
    val startUtc: String = LocalDate.parse(start).atStartOfDay(ZoneId.of("UTC")).toInstant().toString
    val endUtc: String = LocalDate.parse(end).atTime(23, 59, 59).atZone(ZoneId.of("UTC")).toString
    val spark = SparkSession.builder
      .appName("booking-analysis")
      .config(new SparkConf())
      .master("local[4]") //TODO: Remove before finalising!
      .getOrCreate()
    logger.info("Starting Analysis")


    val airports = Airports.toLookup(spark, airportsFile)
    val airportsToCountry = airports.map(m => (m._1, m._2._1))
    val airportsToTimezone = airports.map(m => (m._1, m._2._2))
    val broadcastAirportsToCountry = spark.sparkContext.broadcast(airportsToCountry)
    val broadcastAirportsToTimezone = spark.sparkContext.broadcast(airportsToTimezone)


    val bookings = Bookings.load(spark, bookingsFile)
    val eligibleBookings = Bookings.eligibleForAnalysis(bookings, startUtc, endUtc, broadcastAirportsToCountry.value)
    val flattenedFlights = Bookings.flattenFlights(spark, eligibleBookings, broadcastAirportsToCountry.value)
    val analysisSet = Bookings.toAnalysisDataSet(
      spark,
      flattenedFlights,
      broadcastAirportsToCountry.value,
      broadcastAirportsToTimezone.value)
    val finalResult = Bookings.aggregateToFinalReport(spark, analysisSet)
    finalResult.show(finalResult.count().toInt, false)
    spark.stop()
  }
}
