package cs6240.imagePreprocess

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._
import util.control.Breaks._
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.io.InputStream;
import java.io.BufferedOutputStream
import java.net.URI

object Preprocess {

  // Record contains results we need, such as label, and neighborhood
  case class Record(pos: (Int, (Int, Int, Int)), label: Int, neighbours: Array[Byte])
  // WritableFormat is used for final output, it contains informatin such as how we rotate and mirror image
  case class WritableFormat(transform: Int, label: Int, neighbours: List[Byte])

  def main(args: Array[String]) = {
    val xDim = 512
    val yDim = 512
    val zDim = List[Int](60, 33, 44, 51, 46) // z dimensions are different for 5 images
    // uri is used for differentiating running on local or emr
    // input, output_result, output_image are input/output folders
    // xx, yy, zz are the neighborhood dimension parameters
    // sample_size is the size of output after sampling
    val (uri, input, output_result, output_image) = (args(0), args(1), args(2), args(3))
    val (xx, yy, zz, sample_size) = (args(4).toInt, args(5).toInt, args(6).toInt, args(7).toInt)

    // these functions are used for calculate the coordinates after rotation
    def rotateMapping = Map[Int, (Int, Int) => Int](90 -> ((px, py) => (yy - py - 1) * xx + px),
      180 -> ((px, py) => (xx - px - 1) * yy + (yy - py - 1)),
      270 -> ((px, py) => py * xx + (xx - px - 1)))

    // save the neighborhood image of size xx * yy
    def saveAsPicture(neighbors: Array[Byte], layer: Int, os: BufferedOutputStream) = {
      // extract data points of one layer from neighbors arary
      val pic = ArrayBuffer.empty[Byte]
      for (yi <- 0 until yy) {
        for (xi <- 0 until xx) {
          pic += neighbors(xi * yy * zz + yi * zz + layer)
        }
      }
      val bufferedImage = new BufferedImage(xx, yy, BufferedImage.TYPE_BYTE_GRAY);
      val raster = bufferedImage.getRaster()
      raster.setDataElements(0, 0, xx, yy, pic.toArray)
      ImageIO.write(bufferedImage, "TIFF", os);
      os.flush()
      os.close()
    }

    // mirror the image and create new neighbors array based on given neighbors arary
    // degree means how many degrees the original image has rotated
    def mirrorPixel(neighbors: Array[Byte], degree: Int): Array[Byte] = {
      if (degree != 0 && degree != 90 && degree != 180 && degree != 270)
        throw new IllegalArgumentException("degree should be 0, 90, 180 or 270")
      val res = Array.ofDim[Byte](neighbors.size)
      for (px <- 0 until xx) {
        for (py <- 0 until yy) {
          val srcStart = if (degree % 180 == 0) (px * yy + py) * zz else (py * xx + px) * zz
          val dstStart = if (degree % 180 == 0) ((xx - px - 1) * yy + py) * zz else ((yy - py - 1) * xx + px) * zz
          for (i <- 0 until zz) res(i + dstStart) = neighbors(i + srcStart)
        }
      }
      res
    }

    // rotate the image by degree, and create new neighbors array
    def rotate(neighbors: Array[Byte], degree: Int): Array[Byte] = {
      if (degree != 0 && degree != 90 && degree != 180 && degree != 270)
        throw new IllegalArgumentException("degree should be 0, 90, 180 or 270")
      val res = Array.ofDim[Byte](neighbors.size)
      for (px <- 0 until xx) {
        for (py <- 0 until yy) {
          val srcStart = (px * yy + py) * zz
          val dstStart = rotateMapping(degree)(px, py) * zz
          for (i <- 0 until zz) res(i + dstStart) = neighbors(i + srcStart)
        }
      }
      res
    }

    val conf = new SparkConf()
      .setAppName("image preprocess")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("image preprocess")
      .getOrCreate()
    import spark.implicits._

    // file system and partition number are different when running on local or emr
    val fs = if (uri.equals("localhost")) FileSystem.get(sc.hadoopConfiguration) else FileSystem.get(new URI(uri), sc.hadoopConfiguration)
    val numPartition = if (uri.equals("localhost")) 4 else 300
    
    val seq = List[Int](1, 2, 3, 4, 6) // seq(i) is the prefix of the input image file name
    // load all images by layer, create hashmap that maps (imageId, layerNum) to the brightness arary of this layer
    val imageMap = (0 to 4).par.map { i =>
      Map(
        LoadMultiStack.parse(sc.binaryFiles(input + "/" + seq(i) + "_image.tiff").take(1)(0)._2.open(), xDim, yDim, zDim(i))
          .asScala.toList: _*)
        .map(t => ((seq(i), t._1.toInt), t._2))  //transform from (layer, array) to ((image, layer), array)
    }.reduce(_ ++ _)
    val imageData = sc.broadcast(imageMap)

    // load all the _.dist.tiff files from input folder, and filter out the layers on the edge
    // we don't need layers on the edges, since they has neighborhoods less than xx*yy*zz
    val dist_data = (0 to 4).flatMap { i =>
      LoadMultiStack.parse(sc.binaryFiles(input + "/" + seq(i) + "_dist.tiff").take(1)(0)._2.open(),
        xDim, yDim, zDim(i))
        .asScala.toList
        .map {
        // create label for each pixel based on the distance value
          case (k, v) =>
            val v1 = v.map { t =>
              if (t == 0 || t == 1) 1
              else if (t > 3) 0
              else -1
            }
            ((seq(i), k.toInt), v1)
        }
         // filter out z-axis's edges
        .filter { case (pz, v) => pz._2 >= (zz - 1) / 2 && pz._2 < zDim(i) - (zz - 1) / 2 }
    }
    
    var res = sc.parallelize(dist_data.toList, numPartition).flatMap {
      case (pz, layerDist) =>
        // filter out all the unknow points, and the points on the edges
        layerDist.zipWithIndex.filter {
          t =>
            val (px, py) = (t._2 % xDim, t._2 / xDim)
            t._1 != -1 &&  // if it is unknow points
              px >= xx / 2 && px < xDim - xx / 2 &&   // if the pixel is on x-axis's edges
              py >= yy / 2 && py < yDim - yy / 2 // if the pixel is on y-axis's edges
        }.map {
          case (label, index) =>
            // create the neighborhood array based on the broadcasted imageData
            val neighbors = Array.ofDim[Byte](xx * yy * zz)
            val (px, py) = (index % xDim, index / xDim)
            for (zi <- pz._2 - zz / 2 to pz._2 + zz / 2) {
              val layerBrightness = imageData.value((pz._1, zi))  // all the brightness values on zi layer, pz._1 is the imageId
              val zindex = zi - (pz._2 - zz / 2)
              for (yi <- py - yy / 2 to py + yy / 2) {
                val yindex = (yi - (py - yy / 2)) * zz
                for (xi <- px - xx / 2 to px + xx / 2) {
                  val index = zindex + yindex + (xi - (px - xx / 2)) * yy * zz
                  neighbors(index) = layerBrightness(yi * xDim + xi)   // copy the brightness value to the neighbors array
                }
              }
            }
            Record((pz._1, (px, py, pz._2)), label, neighbors)
        }.toList
    }.toDS().sample(false, 0.1).orderBy(rand()).limit(sample_size).persist()

    // save one foreground pixel's neighbourhoods as images
    val sampleOneForground = res.filter(_.label == 1).first()
    for (layer <- 0 until zz) {
      val output_stream = fs.create(new Path(output_image + "/" + layer + "output_image_fg.tiff"));
      val os = new BufferedOutputStream(output_stream)
      saveAsPicture(sampleOneForground.neighbours.toArray, layer, os)
    }
    println(sampleOneForground.pos)

    // save one background pixel's neighbourhoods as images
    val sampleOneBackGround = res.filter(_.label == 0).first()
    for (layer <- 0 until zz) {
      val output_stream = fs.create(new Path(output_image + "/" + layer + "output_image_bg.tiff"));
      val os = new BufferedOutputStream(output_stream)
      saveAsPicture(sampleOneBackGround.neighbours.toArray, layer, os)
    }
    println(sampleOneBackGround.pos)
    
    res.flatMap { t =>
      // the original sample point, and its mirrored neighbourhood
      val list = ListBuffer[WritableFormat](
          WritableFormat(0, t.label, t.neighbours.toList),
          WritableFormat(1, t.label, mirrorPixel(t.neighbours, 0).toList))
      for (i <- 1 to 3) {
        // rotae the image by i * 90 degree
        list += WritableFormat(i * 2, t.label, rotate(t.neighbours, i * 90).toList)
        // mirror the rotated image
        list += WritableFormat(i * 2 + 1, t.label, mirrorPixel(rotate(t.neighbours, i * 90), i * 90).toList)
      }
      list.toList
    }.write.partitionBy("transform").json(output_result)
  }
}
