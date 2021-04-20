package booking.analysis

import java.time.{Instant, ZoneId}

import booking.analysis.domain.Booking
import booking.analysis.domain.Booking.{Flight, Passenger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object Report {

  def run(spark: SparkSession,
          eligibleBookings: Dataset[Booking],
          broadcastAirportsToCountry: Broadcast[Map[String, String]],
          broadcastAirportsToTimezone: Broadcast[Map[String, String]]
         ) = {
    val flattenedFlights = flattenFlights(spark, eligibleBookings, broadcastAirportsToCountry.value)
    val analysisSet = toAnalysisDataSet(
      spark,
      flattenedFlights,
      broadcastAirportsToCountry.value,
      broadcastAirportsToTimezone.value)
    aggregateToFinalReport(spark, analysisSet)
  }


  case class FlightWithPassengersData(flight: Flight, passengers: Seq[Passenger])

  def flattenFlights(spark: SparkSession, bookings: Dataset[Booking], airportsToCountry: Map[String, String]) = {
    import spark.implicits._
    bookings.flatMap(b => {
      val expanded = b.flights.filter(f => Booking.isKlmFlightOriginatingFromNetherlands(f, airportsToCountry))
      expanded.map(f => FlightWithPassengersData(f, b.passengers))
    })
  }

  def toAnalysisDataSet(spark: SparkSession,
                        bookings: Dataset[FlightWithPassengersData],
                        airportsToCountry: Map[String, String],
                        airportsToTimezone: Map[String, String]): Dataset[(AnalysisKey, AnalysisData)] = {
    import spark.implicits._
    bookings
      .groupByKey(_.flight)
      .mapValues(booking => booking.passengers)
      .mapGroups((flight, passengers) => {
        val uniquePassengers = passengers.flatten.toList.distinct
        (flightToAnalysisKey(flight, airportsToCountry, airportsToTimezone), passengersToAnalysisData(uniquePassengers))
      })
  }

  case class ReportRow(country: String,
                       season: String,
                       weekday: String,
                       noOfPassengers: Int,
                       adults: Int,
                       children: Int,
                       avgWeight: Double,
                       avgAge: Option[Double])

  case class AnalysisData(noOfPassengers: Int,
                          adults: Int,
                          children: Int,
                          totalWeight: Int,
                          ageSum: Int,
                          noOfPassengersWithAgeInfo: Int)

  case class AnalysisKey(country: String, season: String, weekday: String)

  def aggregateToFinalReport(spark: SparkSession,
                             analysisSet: Dataset[(AnalysisKey, AnalysisData)]): Dataset[ReportRow] = {
    import spark.implicits._
    analysisSet
      .rdd
      .reduceByKey((d1, d2) => AnalysisData(
        d1.noOfPassengers + d2.noOfPassengers,
        d1.adults + d2.adults,
        d1.children + d2.children,
        d1.totalWeight + d2.totalWeight,
        d1.ageSum + d2.ageSum,
        d1.noOfPassengers + d2.noOfPassengers
      ))
      .map(pair => analysisToReportRow(pair))
      .sortBy(_.noOfPassengers, false)
      .toDS()
  }

  private[analysis] def analysisToReportRow(pair: (AnalysisKey, AnalysisData)): ReportRow = {
    val key = pair._1
    val data = pair._2
    val avgAge: Option[Double] = if (data.noOfPassengersWithAgeInfo > 0) {
      Some(data.ageSum / data.noOfPassengersWithAgeInfo)
    } else None
    ReportRow(key.country, key.season, key.weekday,
      data.noOfPassengers, data.adults, data.children, data.totalWeight / data.noOfPassengers, avgAge)
  }

  private[analysis] def flightToAnalysisKey(flight: Flight,
                                            airportsToCountry: Map[String, String],
                                            airportsToTimezone: Map[String, String]): AnalysisKey = {
    val seasons = Seq("Winter", "Winter", "Spring", "Spring", "Summer", "Summer",
      "Summer", "Summer", "Fall", "Fall", "Winter", "Winter")
    val instant = Instant
      .parse(flight.departureDate)
      .atZone(ZoneId.of(airportsToTimezone.get(flight.origin).getOrElse("UTC")))
    val season = seasons(instant.getMonthValue - 1)
    val weekday = instant.getDayOfWeek().toString
    AnalysisKey(airportsToCountry.get(flight.destination).getOrElse("Unknown"), season, weekday)
  }

  private[analysis] def passengersToAnalysisData(uniquePassengers: Seq[Passenger]): AnalysisData = {
    val noOfPassengers = uniquePassengers.size
    val adults = uniquePassengers.filter(_.category == "ADT").size
    val children = uniquePassengers.filter(_.category == "CHD").size
    val totalWeight = uniquePassengers.map(_.weight).sum
    val definedAges = uniquePassengers.filter(_.age.isDefined)
    val ageSum = definedAges.map(_.age.get).sum
    val noOfPassengersWithAgeInfo = definedAges.size
    AnalysisData(noOfPassengers, adults, children, totalWeight, ageSum, noOfPassengersWithAgeInfo)
  }
}
