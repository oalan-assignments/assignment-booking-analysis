package booking.analysis

import booking.analysis.Booking.{Flight, Passenger}
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

case class Booking(timestamp: String, passengers: Seq[Passenger], flights: Seq[Flight])

object Booking {

  val logger = Logger.getLogger(Booking.getClass)

  case class Passenger(uci: String,
                       `type`: String,
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
        p("age").numOpt.map(_.intValue()),
        p("weight").num.intValue()
      ))
      val flights: Seq[Flight] = productsList.arr.map(p => Flight(
        p("bookingStatus").str,
        p("flight")("marketingAirline").str,
        p("flight")("originAirport").str,
        p("flight")("destinationAirport").str,
        p("flight")("departureDate").str,
        p("flight")("arrivalDate").str
      ))
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
}
