import booking.analysis.Booking
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

val base = s"${System.getProperty("user.home")}/dev/assignment-booking-analysis/data"
val spark = SparkSession.builder
  .appName("booking-analysis")
  .config(new SparkConf())
  .master("local[4]")
  .getOrCreate()


case class AirportInfo(iata: String, country: String, timezone: Double)
val bookingsFile = s"$base/bookings/booking.json"
val airports = s"$base/airports/airports.dat"
import spark.implicits._
val logData = spark.read
  .format("csv")
  .option("header", "false")
  .option("inferSchema", "true")
  .load(airports)
  .toDF(
    "id", "name", "city", "country", "iata", "icao", "lat", "lon", "alt", "timezone", "dst", "tz", "airportType", "source")
  .select("iata", "country", "timezone")
  .filter(col("iata") =!= "\\N")
  .filter(col("timezone") =!= "\\N")
  .withColumn("timezone", col("timezone").cast(DoubleType))
  .as[AirportInfo]
  .collect()
  .map(a => a.iata -> (a.country, a.timezone))
  .toMap


val airportsToCountry =  logData.map(m => (m._1, m._2._1))
val broadCastAirports = spark.sparkContext.broadcast(airportsToCountry)



import spark.implicits._
val bookings = spark.read.textFile(bookingsFile).map(Booking.fromJson(_))
bookings.count()
val definedBookings = bookings.filter(_.isDefined).map(_.get)
val klFlights = definedBookings.filter(b => Booking.containsKlmFlight(b))
klFlights.count()
val confirmedKLFlights = klFlights.filter(b => Booking.isConfirmed(b))
confirmedKLFlights.count()
val confirmedKLFlightsFromNL = klFlights.filter(b =>
    Booking.isOriginatingFromNetherlands(b, broadCastAirports.value))
confirmedKLFlightsFromNL.count()
confirmedKLFlightsFromNL.select("flights").show(100, false)














