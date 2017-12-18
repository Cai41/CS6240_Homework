package cs6240.pagerankScala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object PageRank {
  def main(args: Array[String]) = {
    val ALPHA = 0.15

    // calculate the page rank from formula
    def calcRank(totalNodes: Long, delta: Double)(value: Double): Double = {
      ALPHA / totalNodes + (1 - ALPHA) * (value + delta / totalNodes)
    }

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")
    val sc = new SparkContext(conf)

    val danglingDelta = sc.doubleAccumulator("danglingDelta")
    val graph = sc.textFile(args(0))
      .flatMap(line => Preprocessor.map(line).asScala.toList) // preprocess html file
      .map { case (k, v) => (k, if (v == null) Nil else v.asScala.toList.filter(!_.equals(k))) } // convert from java list to scala list, and remove self-link
      .reduceByKey((a, b) => a match {
        case Nil => b
        case _ if b == Nil => a
        case _ => a ++ b
      }) // merge all out-links from the same page
      .persist
    val totalNodes = graph.count

    var delta = 0.0
    var pageRanks = graph.mapValues(t => 1.0 / totalNodes) // initialize the page rank value
    var cachedRddId = -1

    for (i <- 1 to 10) {
      val tmp = pageRanks
        .join(graph) // join the graph with the page ranks
        .flatMap {
          // emit the contributions of each out-link, also add the pagerank for dangling nodes
          // to the global counter
          case (k, v) =>
            val tail = List((k, 0.0))
            v._2 match {
              case Nil => {
                danglingDelta.add(v._1)
                tail
              }
              case _ => v._2.map(name => (name, v._1 / v._2.size)) ++ tail
            }
        }
        // aggregate all contributions to the same page
        .foldByKey(0.0)(_ + _).persist
      tmp.count
      // remove the previous persisted RDD from cache, since we have a newer one(tmp)
      if (cachedRddId != -1) sc.getPersistentRDDs(cachedRddId).unpersist()
      cachedRddId = tmp.id
      delta = danglingDelta.value
      danglingDelta.reset()
      // calculate the new RDD
      pageRanks = tmp.mapValues(calcRank(totalNodes, delta))
    }
    // take top 100 page and save to file
    val topK = pageRanks.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
    sc.parallelize(topK, 1).saveAsTextFile(args(1))
    sc.stop
  }
}
