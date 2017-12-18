package CS6240.WeatherScala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import java.io.File

object Analyze {
  // accumulate data structrue to store temperature information
  case class AccumEntry(maxSum: Int, maxCt: Int, minSum: Int, minCt: Int)

  // combine two AccumEntry into one AccumEntry
  def combEntry(r1: AccumEntry, r2: AccumEntry): AccumEntry = {
    AccumEntry(r1.maxSum + r2.maxSum, r1.maxCt + r2.maxCt, r2.minSum + r2.minSum, r2.minCt + r2.maxSum)
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

  // compute the average temperature
  def AccumEntryToAve(v: AccumEntry): (Double, Double) = {
    (if (v.maxCt == 0) Double.NaN else 1.0 * v.maxSum / v.maxCt,
      if (v.minCt == 0) Double.NaN else 1.0 * v.minSum / v.minCt)
  }

  // function without combiner
  def NoCombiner(rdd: RDD[(String, (Option[Int], Option[Int]))]): RDD[(String, (Double, Double))] = {
    rdd.map(InputToAccumEntry)
      .groupByKey()
      // convert list of entries to averages
      .mapValues(entries => AccumEntryToAve(entries.foldLeft(AccumEntry(0, 0, 0, 0))(combEntry)))
  }

  // since reduceByKey will automatically combine in each partition, so this function
  // has combiner implicit
  def WithCombiner(rdd: RDD[(String, (Option[Int], Option[Int]))]): RDD[(String, (Double, Double))] = {
    rdd.map(InputToAccumEntry)
      .reduceByKey(combEntry)
      .mapValues(AccumEntryToAve)
  }

  // function has in-mapper combiner in each mapper task
  def inMapperCombiner(rdd: RDD[(String, (Option[Int], Option[Int]))]): RDD[(String, (Double, Double))] = {
    rdd.mapPartitions { records =>
      // set up a hasmap and accumulate result in hashmap
      val map: HashMap[String, AccumEntry] = HashMap()
      records.foreach(r => map += (r._1 -> combEntry(map.getOrElse(r._1, AccumEntry(0, 0, 0, 0)), InputToAccumEntry(r)._2)))
      map.iterator
    }.reduceByKey(combEntry).mapValues(AccumEntryToAve)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Weather Data")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    val test = sc.textFile(args(1))
    // filter out "TMAX"/"TMIN", and convert to (station. maxtemp, mintemp)
    val res = test.map(line => line.split(","))
      .filter(arr => arr(2) == "TMAX" || arr(2) == "TMIN")
      .map(lintToInputEntry)
    // map command line argument to each function: NoCombiner/WithCombiner/inMapperCombiner
    def mapFunctions = Map[String, 
      ((RDD[(String, (Option[Int], Option[Int]))]) => RDD[(String, (Double, Double))])](
          "NoCombiner" -> NoCombiner,
          "WithCombiner" -> WithCombiner,
          "InMapperCombiner" -> inMapperCombiner
          )
    // execute the corresponding function and save to file
    mapFunctions(args(0))(res).saveAsTextFile(args(2))
  }
}