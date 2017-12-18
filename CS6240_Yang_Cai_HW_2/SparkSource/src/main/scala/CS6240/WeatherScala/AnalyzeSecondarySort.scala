package CS6240.WeatherScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner

object AnalyzeSecondarySort {
   // accumulate data structrue to store temperature information
  case class AccumEntry(maxSum: Int, maxCt: Int, minSum: Int, minCt: Int)

  // accumulate data structrue for one year
  case class YearAccumEntry(year: Int, entry: AccumEntry)

  // key in the secondary sort
  case class SecondaryKey(stationId: String, year: Int)

  // key comparator, first sort on station and then by year
  object SecondaryKey {
    implicit def orderByStationAndYear[A <: SecondaryKey]: Ordering[A] = {
      Ordering.by(e => (e.stationId, -1 * e.year))
    }
  }

  // key partitioner based onlt on station id
  class SecondaryPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions
    val delegate = new HashPartitioner(partitions)
    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[SecondaryKey]
      delegate.getPartition(k.stationId)
    }
  }

  // secondary sort, make use of repartitionAndSortWithinPartitions. And by calling mapPartitions
  // we are aggragating records for each stationa and year
  def SecondarySort(rdd: RDD[(SecondaryKey, AccumEntry)]): RDD[(String, List[(Int, Double, Double)])] = {
    rdd.repartitionAndSortWithinPartitions(new SecondaryPartitioner(rdd.partitions.size))
      .mapPartitions { iter =>
        val map = HashMap[String, List[(Int, Double, Double)]]()
        val last = iter.foldLeft(("Dummy", YearAccumEntry(0, AccumEntry(0, 0, 0, 0)))) {
          (accm, ele) =>
            if (accm._1 == ele._1.stationId) {
              val id = accm._1
              if (accm._2.year == ele._1.year) {
                // if both year and station are the same, we accumulate the data
                val year = accm._2.year
                val oldAccmEntry = accm._2.entry
                val newAccmEntry = AccumEntry(oldAccmEntry.maxSum + ele._2.maxSum,
                  oldAccmEntry.maxCt + ele._2.maxCt,
                  oldAccmEntry.minSum + ele._2.minSum,
                  oldAccmEntry.minCt + ele._2.minCt)
                (id, YearAccumEntry(year, newAccmEntry))
              } else {
                // if it is different year but same station, we comnpute the averages and append 
                // to stations's list
                val lst = map.getOrElse(id, List())
                val e = accm._2.entry
                map += (id -> ((accm._2.year, e.maxSum * 1.0 / e.maxCt, e.minSum * 1.0 / e.minCt) :: lst))
                (id, YearAccumEntry(ele._1.year, ele._2))
              }
            } else {
              // if it a different station, we compute the average and put a new record in hasmap
              if (accm._1 != "Dummy") {
                val lst = map.getOrElse(accm._1, List())
                val e = accm._2.entry
                map += (accm._1 -> ((accm._2.year, e.maxSum * 1.0 / e.maxCt, e.minSum * 1.0 / e.minCt) :: lst))
              }
              (ele._1.stationId, YearAccumEntry(ele._1.year, ele._2))
            }
        }
        map.iterator
      }
  }

  // convert one line in .csv to (station, maxtemp, mintemp)
  def lintToInputEntry(arr: Array[String]): (String, (Option[Int], Option[Int])) = {
    (arr(0),
      (if (arr(2) == "TMAX") Some(arr(3).toInt) else None,
        if (arr(2) == "TMIN") Some(arr(3).toInt) else None))
  }

  // convert (station, maxtemp, mintemp) to accumulate data structrue
  def InputToAccumEntry(r: (String, (Option[Int], Option[Int]))): (String, AccumEntry) = {
    (r._1, if (r._2._1.isEmpty) AccumEntry(0, 0, r._2._2.get, 1) else AccumEntry(r._2._1.get, 1, 0, 0))
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val test = sc.textFile(args(0))
    val res = test.map(line => line.split(","))
      .filter(arr => arr(2) == "TMAX" || arr(2) == "TMIN")
      .map { arr =>
        val secondaryKey = SecondaryKey(arr(0), arr(1).substring(0, 4).toInt)
        val accumEntry = InputToAccumEntry(lintToInputEntry(arr))._2
        (secondaryKey, accumEntry)
      }
    SecondarySort(res).saveAsTextFile(args(1))
  }
}