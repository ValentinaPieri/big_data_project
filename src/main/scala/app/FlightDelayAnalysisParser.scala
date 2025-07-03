package app

import org.apache.spark.rdd.RDD
import java.util.Calendar


object FlightDelayAnalysisParser {
  private def getInt(s: String): Int = Option(s).map(_.trim).filter(_.matches("-?\\d+")).map(_.toInt).getOrElse(0)
  private def getDouble(s: String): Double = Option(s).map(_.trim).filter(_.matches("-?\\d+(\\.\\d+)?")).map(_.toDouble).getOrElse(0.0)
  private def getBoolean(s: String): Boolean = Option(s).map(_.trim.toLowerCase).contains("true")

  def parseRow(row: String): Option[(String, String, String, String, Boolean, Boolean, Int, Double, Double, Int, Int)] = {
    try {
      val cols = row.split(",", -1).map(_.trim.stripPrefix("\"").stripSuffix("\""))
      val flightDate = cols(0)
      val airline    = cols(1)
      val origin     = cols(2)
      val dest       = cols(3)
      val cancelled  = getBoolean(cols(4))
      val diverted   = getBoolean(cols(5))
      val depTime    = getInt(cols(7))
      val depDelay   = getDouble(cols(9))
      val airTime    = getDouble(cols(12))
      val year       = getInt(cols(16))
      val month      = getInt(cols(17))
      Some((flightDate, airline, origin, dest, cancelled, diverted, depTime, depDelay, airTime, year, month))
    } catch {
      case e: Exception =>
        println(s"Error parsing row: $$row -> $${e.getMessage}")
        None
    }
  }

  def parseRDD(rdd: RDD[String]): RDD[(String, String, String, String, Boolean, Boolean, Int, Double, Double, Int, Int)] = {
    rdd.flatMap(row => parseRow(row))
  }
}
