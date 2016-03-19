package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.Buffer

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input file", required = true)
  val output = opt[String](descr = "output dir", required = true)
  val model = opt[String](descr = "model dir", required = true)
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // w is the weight vector
  var w = scala.collection.mutable.Map[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Buffer[Int]): Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input path" + args.input())
    log.info("Output dir" + args.output())
    log.info("Model dir" + args.model())

    val conf = new SparkConf().setAppName("Apply Spam Classifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val modelFile = sc.textFile(args.model() + "/part-00000")

    val modelMap = modelFile.map(line => {
      val stringArray = line.split("\\,")
      val feature = stringArray(0).drop(1).toInt
      val weight = stringArray(1).dropRight(1).toDouble
      w(feature) = weight
    })

    val result = textFile.map(line => {
      val trainingInstanceArray = line.split(" ")
      val docid = trainingInstanceArray(0)
      val label = trainingInstanceArray(1)

      val featuresStringArray = trainingInstanceArray.toBuffer

      featuresStringArray -= docid
      featuresStringArray -= label

      val features = featuresStringArray.map(_.toInt)

      val spamminessScore = spamminess(features)

      if (spamminessScore > 0)
        (docid, label, spamminessScore, "spam")
      else
        (docid, label, spamminessScore, "ham")

//      (0, (docid, label, featuresStringArray))
    })
//      .groupByKey(1)
//      .map(pair => {
//
//        val pairList = pair._2.toList
//
//        // each training instance
//        pairList.foreach(tuple => {
//
//          // document ID
//          val docId = tuple._1
//          // label
//          val label = tuple._2
//
//          val featuresString = tuple._3
//          // features
//          val features = featuresString.map(_.toInt)
//
//        })
//      })

    result.saveAsTextFile(args.output())


  }
}