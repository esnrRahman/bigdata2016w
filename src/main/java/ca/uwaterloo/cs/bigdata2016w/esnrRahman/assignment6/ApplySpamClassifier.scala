package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

import scala.io.Source

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input file", required = true)
  val output = opt[String](descr = "output dir", required = true)
  val model = opt[String](descr = "model dir", required = true)
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

//  var w = scala.collection.mutable.Map[Int, Double]()

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

    val modelData = modelFile.map(line => {
      val stringArray = line.split(",")
      val feature = stringArray(0).drop(1).toInt
      val weight = stringArray(1).dropRight(1).toDouble
      (feature, weight)
    })

    val test = modelData.collect.toMap

    val brval = sc.broadcast(test)


    //    for(line <- Source.fromFile(args.model() + "/part-00000").getLines()) {
//            val stringArray = line.split(",")
//            val feature = stringArray(0).drop(1).toInt
//            val weight = stringArray(1).dropRight(1).toDouble
//            w.put(feature, weight)
//    }

//    val test = sc.broadcast(w)


    val result = textFile.map(line => {
      val trainingInstanceArray = line.split(" ")
      val docid = trainingInstanceArray(0)
      val label = trainingInstanceArray(1)

      var spamminessScore = 0d

      for (x <- 2 until trainingInstanceArray.length) {
        val feature = trainingInstanceArray(x).toInt
        if (brval.value.contains(feature)) spamminessScore += brval.value(feature)
      }

      //      val featuresString = trainingInstanceArray.slice(2, trainingInstanceArray.length)
      //      val features = featuresString.map(_.toInt)


      //      features.foreach(f => if (w.contains(f)) spamminessScore += w(f))

      if (spamminessScore > 0)
        (docid, label, spamminessScore, "spam")
      else
        (docid, label, spamminessScore, "ham")
    })

    result.saveAsTextFile(args.output())


  }
}