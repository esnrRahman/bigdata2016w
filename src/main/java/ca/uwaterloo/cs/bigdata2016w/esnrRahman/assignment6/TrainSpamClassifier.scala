package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.Buffer

class Conf1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input file", required = true)
  val model = opt[String](descr = "output dir", required = true)
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // w is the weight vector (make sure the variable is within scope)
  var w = scala.collection.mutable.Map[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Buffer[Int]): Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf1(argv)

    log.info("Input path" + args.input())
    log.info("Output dir" + args.model())

    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val trained = textFile.map(line => {
      val trainingInstanceArray = line.split(" ")
      val docid = trainingInstanceArray(0)
      val label = trainingInstanceArray(1)

      val isSpam = if (label == "spam") 1 else 0

      val features = trainingInstanceArray.toBuffer
      features -= docid
      features -= label

//      println("EHSAN 1 -> " + features)
      (0, (docid, isSpam, features))
    })
      .groupByKey(1)
      .map(pair => {
        // Then run the trainer...

        // This is the main learner:
        val delta = 0.002

        val pairList = pair._2.toList

        // For each instance...
        val isSpam = pairList(1)
        // label
        val featuresString = pairList(2) // feature vector of the training instance

        println("EHSAN 1 -> " + featuresString)

//        val features = featuresString.map(_.toInt)
////        println("EHSAN 2 -> " + pair._2.iterator.next()._1)
//
//
//        // Update the weights as follows:
//        val score = spamminess(features)
//        val prob = 1.0 / (1 + Math.exp(-score))
//        features.foreach(f => {
//          if (w.contains(f)) {
//            w(f) += (isSpam - prob) * delta
//          } else {
////            val result = (isSpam - prob) * delta
////            w += (f -> result)
//            w(f) = (isSpam - prob) * delta
//          }
//        })
//        w
      })

    trained.saveAsTextFile(args.model())


  }
}