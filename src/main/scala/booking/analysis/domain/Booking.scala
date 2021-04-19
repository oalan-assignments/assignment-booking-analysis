package booking.analysis.domain

import booking.analysis.domain.Booking.{Flight, Passenger}
import org.apache.log4j.Logger
import ujson.Value

import scala.util.{Failure, Success, Try}

case class Booking(timestamp: String, passengers: Seq[Passenger], flights: Seq[Flight])

object Booking {

  val logger = Logger.getLogger(Booking.getClass)

  case class Passenger(uci: String,
                       category: String,
                       age: Option[Int],
                       weight: Int)

  case class Flight(status: String,
                    airline: String,
                    origin: String,
                    destination: String,
                    departureDate: String,
                    arrivalDate: String)

  def fromJson(line: String): Option[Booking] = {
    val parseAttempt = Try {
      val record = ujson.read(line)
      val timestamp = record("timestamp").str
      val travelRecord = record("event")("DataElement")("travelrecord")
      val passengersList = travelRecord("passengersList")
      val productsList = travelRecord("productsList")
      val passengers: Seq[Passenger] = passengersList.arr.map(p => Passenger(
        p("uci").str,
        p("passengerType").str,
        extractAgeIfExists(p),
        p("weight").num.intValue()
      ))
      assert(passengers.nonEmpty, "Passenger information can not be empty")
      val flights: Seq[Flight] = productsList.arr.map(f => Flight(
        f("bookingStatus").str,
        f("flight")("marketingAirline").str,
        f("flight")("originAirport").str,
        f("flight")("destinationAirport").str,
        f("flight")("departureDate").str,
        f("flight")("arrivalDate").str
      ))
        .sortBy(_.departureDate)
      assert(flights.nonEmpty, "Flight information can not be empty")
      Booking(timestamp, passengers, flights)
    }
    parseAttempt match {
      case Success(booking) => Some(booking)
      case Failure(exception) => {
        logger.error(s"Error while reading a json booking line: $line", exception)
        None
      }
    }
  }

  def isConfirmed(booking: Booking): Boolean = {
    booking.flights.last.status == "CONFIRMED"
  }

  def isEligibleForAnalysis(booking: Booking,
                            startUtc: String,
                            endUtc: String,
                            airportCountryLookup: Map[String, String]): Boolean = {
    booking.flights.exists(f =>
      flewInPeriod(f, startUtc, endUtc) &&
        isKlmFlightOriginatingFromNetherlands(f, airportCountryLookup))
  }

  def flewInPeriod(flight: Flight, startUtc: String, endUtc: String): Boolean = {
    flight.departureDate >= startUtc && flight.departureDate <= endUtc
  }

  def isKlmFlightOriginatingFromNetherlands(flight: Flight, airportCountryLookup: Map[String, String]): Boolean = {
    airportCountryLookup.get(flight.origin) match {
      case Some(country) => country == "Netherlands" && flight.airline == "KL"
      case _ => false
    }
  }

  private def extractAgeIfExists(value: Value): Option[Int] = {
    val attempt = Try(value("age").numOpt.map(_.intValue()))
    attempt match {
      case Success(result) => result
      case Failure(_) => None
    }
  }
}
