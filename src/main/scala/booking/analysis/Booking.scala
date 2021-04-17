package booking.analysis

import booking.analysis.Booking.{Flight, Passenger}

case class Booking(timestamp: String, passengers: Seq[Passenger], flights: Seq[Flight])

object Booking {

  case class Passenger(uci: String,
                       `type`: String,
                       age: Option[Int],
                       weight: Int)

  case class Flight(airline: String,
                    origin: String,
                    destination: String,
                    departureDate: String,
                    arrivalDate: String)

  def fromJson(line: String): Option[Booking] = {
    val record = ujson.read(line)
    val timestamp = record("timestamp")
    val travelRecord = record("event")("DataElement")("travelrecord")
    val passengersList = travelRecord("passengersList")
    val productsList = travelRecord("productsList")

    None
  }

}
