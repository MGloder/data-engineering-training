package workshop.schema

import java.sql.Date
import java.text.SimpleDateFormat
import java.util
import java.util.Locale

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try

case class TaxiRide(val rideId: Long,
                    val time: Date,
                    val isStart: Boolean,
                    val location: GeoPoint,
                    val passengerCount: Short,
                    val travelDistance: Float) {
  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("NYC TaxiL ")
    sb.append(" rideId: " + rideId)
    sb.append(" datetime: " + time)
    sb.append(" isStart: " + isStart)
    sb.append(" location: " + GeoPoint)
    sb.append(" passengerCount: " + passengerCount)
    sb.append(" travelDistance: " + travelDistance)
    sb.toString()
  }
}

object TaxiRide {

  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC

  def fromString(line: String): TaxiRide = {
    val tokens: Array[String] = line.split(",")
    if (tokens.length != 7) {
      throw new RuntimeException("Invalid record: " + line)
    }

    Try {
      val rideId = tokens(0).toLong
      val time: Date = new Date(new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").parse(tokens(1)).getTime)
      val isStart = tokens(2) == "START"
      val lon = if (tokens(3).length > 0) tokens(3).toDouble else 0.0
      val lat = if (tokens(4).length > 0) tokens(4).toDouble else 0.0
      val passengerCnt = tokens(5).toShort
      val travelDistance = if (tokens(6).length > 0) tokens(6).toFloat else 0.0f

      new TaxiRide(rideId, time, isStart, GeoPoint(lon, lat), passengerCnt, travelDistance)
    } match {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }


  }
}

