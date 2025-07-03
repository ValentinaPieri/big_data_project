package app

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop
import app.FlightDelayAnalysisParser
import utils._


object FlightDelayAnalysisApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlightDelayAnalysis")
      .getOrCreate()

    val sc = spark.sparkContext

    // Paths for each year

    val path2019 = "/datasets/Combined_Flights_2019.csv"
    val path2020 = "/datasets/Combined_Flights_2020.csv"
    val path2021 = "/datasets/Combined_Flights_2021.csv"
    val path2022 = "/datasets/Combined_Flights_2022.csv"

    val flights2019RDD = FlightDelayAnalysisParser.parseRDD(sc.textFile(Commons.getDatasetPath(path2019)))
    val flights2020RDD = FlightDelayAnalysisParser.parseRDD(sc.textFile(Commons.getDatasetPath(path2020)))
    val flights2021RDD = FlightDelayAnalysisParser.parseRDD(sc.textFile(Commons.getDatasetPath(path2021)))
    val flights2022RDD = FlightDelayAnalysisParser.parseRDD(sc.textFile(Commons.getDatasetPath(path2022)))

    if (args.nonEmpty && args(0).equalsIgnoreCase("opt")) {
      runAllYears(spark, flights2019RDD, flights2020RDD, flights2021RDD, flights2022RDD)
    } else if (args.nonEmpty && args(0).equalsIgnoreCase("non-opt")) {
      nonOptimized(spark, flights2019RDD, flights2020RDD, flights2021RDD, flights2022RDD)
    }
  }

  def nonOptimized ( spark: SparkSession,
                    flights2019RDD: RDD[(String,String,String,String,Boolean,Boolean,Int,Double,Double,Int,Int)],
                    flights2020RDD: RDD[(String,String,String,String,Boolean,Boolean,Int,Double,Double,Int,Int)],
                    flights2021RDD: RDD[(String,String,String,String,Boolean,Boolean,Int,Double,Double,Int,Int)],
                    flights2022RDD: RDD[(String,String,String,String,Boolean,Boolean,Int,Double,Double,Int,Int)]
                  ): Unit = {

    // Run analysis for each year
    val res2019 = runYear(flights2019RDD, "2019")
    val res2020 = runYear(flights2020RDD, "2020")
    val res2021 = runYear(flights2021RDD, "2021")
    val res2022 = runYear(flights2022RDD, "2022")

    val allResults = spark.sparkContext.union(res2019, res2020, res2021, res2022)
    allResults.collect()
  }

  def runYear(
               flights: RDD[(String,String,String,String,Boolean,Boolean,Int,Double,Double,Int,Int)],
               flightYear: String
             ): RDD[String] = {

    // Average depDelay per airline
    val airlineStats = flights
      .map { case (_, airline, _, _, _, _, _, depDelay, _, _, _) => (airline, (depDelay, 1))}
      .groupByKey()

    val arlineMap = airlineStats
      .mapValues { iter => iter.foldLeft((0.0, 0)) { case ((sum, cnt), (delay, one)) => (sum + delay, cnt + one)}}

    val airlineAvgDelay = arlineMap.mapValues { case (sum, cnt) => sum / cnt }

    // Build class
    val airlineClass = airlineAvgDelay.mapValues {
      case d if d < 5.0  => "Good"
      case d if d < 15.0 => "Average"
      case _             => "Bad"
    }

    // Re-key flights by airline
    val flightsByAirline = flights
      .map { case (date, airline, origin, dest, cancelled, diverted, depTime, depDelay, airTime, year, month) =>
        (airline, (date, origin, dest, cancelled, diverted, depTime, depDelay, airTime, year, month))
      }

    // Join
    val taggedFlights = flightsByAirline
      .join(airlineClass)
      .map { case (_, (flight, cls)) =>
        val (date, origin, dest, cancelled, diverted, depTime, depDelay, airTime, year, month) = flight
        ((origin, dest, cls), 1)
      }

    // Count by route+class via groupByKey
    val routeClassCounts = (taggedFlights
      .groupByKey()
      .mapValues(_.size)
      )

    val perClassLists = routeClassCounts.map {
      case ((o, d, c), cnt) => (c, List(((o, d), cnt)))
    }

    val classOrder = Map("Bad" -> 1, "Average" -> 2, "Good" -> 3)

    // Top5 per class
    val top5List = perClassLists
      .reduceByKey { (l1, l2) => (l1 ++ l2)
        .sortBy(-_._2).take(5)
      }
      .sortBy { case (cls, _) => classOrder(cls) }

    val top5 = top5List
      .flatMap { case (cls, list) =>
        list.map { case ((o, d), cnt) =>
          f"$cls%-7s | $o%-4s - $d%-4s | Count: $cnt%6d"
        }
      }

    top5
  }



  def runAllYears(
                   spark: SparkSession,
                   flights2019RDD: RDD[(String, String, String, String, Boolean, Boolean, Int, Double, Double, Int, Int)],
                   flights2020RDD: RDD[(String, String, String, String, Boolean, Boolean, Int, Double, Double, Int, Int)],
                   flights2021RDD: RDD[(String, String, String, String, Boolean, Boolean, Int, Double, Double, Int, Int)],
                   flights2022RDD: RDD[(String, String, String, String, Boolean, Boolean, Int, Double, Double, Int, Int)],
                   topN: Int = 5
                 ): Unit = {

    val allFlights = spark.sparkContext.union(flights2019RDD, flights2020RDD, flights2021RDD, flights2022RDD).cache()

    // Average depDelay per airline
    val airlineAvgDelay = allFlights
      .map { case (_, airline, _, _, _, _, _, depDelay, _, _, _) => (airline, (depDelay, 1)) }
      .reduceByKey { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
      .mapValues { case (sum, cnt) => sum / cnt }

    // Build class
    val airlineClass = airlineAvgDelay.mapValues {
      case d if d < 5.0  => "Good"
      case d if d < 15.0 => "Average"
      case _             => "Bad"
    }

    // Re-key flights by airline
    val flightsByAirline = allFlights
      .map { case (_, airline, origin, dest, _, _, _, _, _, _, _) =>
        (airline, (origin, dest))
      }.join(airlineClass)
      .map { case (_, ((o, d), cls)) => ((o, d, cls), 1) }

    // Count by route+class with reduceByKey
    val routeClassCounts = flightsByAirline
      .reduceByKey(_ + _)

    val perClassLists = routeClassCounts
      .map { case ((o, d, cls), cnt) => (cls, ((o, d), cnt))}

    val classOrder = Map("Bad" -> 1, "Average" -> 2, "Good" -> 3)

    // Top5 with aggregateByKey
    val top5 = perClassLists
      .aggregateByKey(List.empty[((String, String), Int)])(
        (acc, v) => (acc :+ v)
          .sortBy(-_._2)
          .take(5),
        (acc1, acc2) => (acc1 ++ acc2)
          .sortBy(-_._2)
          .take(5)
      ).mapValues(_.toSeq)

    // Top5 print
    top5
      .flatMap { case (cls, seqRoutes) =>
        seqRoutes.map { case ((o, d), cnt) =>
          f"$cls%-7s | $o%-4s - $d%-4s | Count: $cnt%6d"
        }
      }
      .collect()

    // Cleanup cached RDDs
    allFlights.unpersist()
  }
}
