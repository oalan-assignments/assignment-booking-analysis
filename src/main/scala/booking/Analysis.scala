package booking

import java.io.{File, FileOutputStream}
import java.util.UUID

import booking.analysis.{Airports, Bookings}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analysis {

  val logger = Logger.getLogger(Analysis.getClass)

  def main(args: Array[String]): Unit = {
//    val Array(start, end, bookingsPath) = args
//    val startUtc: String = LocalDate.parse(start).atStartOfDay(ZoneId.of("UTC")).toInstant().toString
//    val endUtc: String = LocalDate.parse(end).atTime(23, 59, 59).atZone(ZoneId.of("UTC")).toString
    val airportsFile = pathOfAirportsDataFile()
    val spark = SparkSession.builder
      .appName("booking-analysis")
      .config(new SparkConf())
      .master("local[4]") //TODO: Remove before finalising!
      .getOrCreate()
    logger.info("Starting Analysis")

    val base = s"${System.getProperty("user.home")}/dev/assignment-booking-analysis/data"
    val bookingsFile = s"$base/bookings/booking.json"
//    val airportsFile = s"$base/airports/airports.dat"
    val airports = Airports.toLookup(spark, airportsFile)
    val airportsToCountry = airports.map(m => (m._1, m._2._1))
    val airportsToTimezone = airports.map(m => (m._1, m._2._2))
    val broadcastAirportsToCountry = spark.sparkContext.broadcast(airportsToCountry)
    val broadcastAirportsToTimezone = spark.sparkContext.broadcast(airportsToTimezone)
    val startUtc = "2010-02-05T13:05:00Z"
    val endUtc = "2025-02-05T13:05:00Z"

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

  private def pathOfAirportsDataFile(): String = {
    import resource._
    val randomFileName = UUID.randomUUID().toString + ".dat"
    val tempFolder = System.getProperty("java.io.tmpdir")
    val logFile = new File(tempFolder, randomFileName)
    val copyFromJar = for {input <- managed(getClass.getResourceAsStream("/airports.dat"))
         output <- managed(new FileOutputStream(logFile))
         } yield {
      input.transferTo(output)
    }
    val result : Either[List[Throwable], Long]= copyFromJar.acquireFor(identity)
    result match {
      case Left(exceptions) => {
        val dueTo = exceptions.head
        logger.error(s"Error while reading airports data. Due to: $dueTo")
        throw new RuntimeException()
      }
      case Right(_) => logFile.getPath
    }
  }


}
