package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input file", required = true)
  val output = opt[String](descr = "output dir", required = true)
  val model = opt[String](descr = "model dir", required = true)
  val method = opt[String](descr = "method type", required = true)
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  var wX = scala.collection.mutable.Map[Int, Double]()
  var wY = scala.collection.mutable.Map[Int, Double]()
  var wBritney = scala.collection.mutable.Map[Int, Double]()

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input path:" + args.input())
    log.info("Output dir:" + args.output())
    log.info("Model dir:" + args.model())
    log.info("Ensemble technique:" + args.method())

    val conf = new SparkConf().setAppName("Apply Ensemble Spam Classifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    // process file x
    val modelXFile = sc.textFile(args.model() + "/part-00000")

    val modelXData = modelXFile.map(line => {
      val stringArray = line.split(",")
      val feature = stringArray(0).drop(1).toInt
      val weight = stringArray(1).dropRight(1).toDouble
      (feature, weight)
    })

    val testX = modelXData.collect.toMap
    val brvalX = sc.broadcast(testX)

    // process file y
    val modelYFile = sc.textFile(args.model() + "/part-00001")

    val modelYData = modelYFile.map(line => {
      val stringArray = line.split(",")
      val feature = stringArray(0).drop(1).toInt
      val weight = stringArray(1).dropRight(1).toDouble
      (feature, weight)
    })

    val testY = modelYData.collect.toMap
    val brvalY = sc.broadcast(testY)

    // process file britney
    val modelBritneyFile = sc.textFile(args.model() + "/part-00002")

    val modelBritneyData = modelBritneyFile.map(line => {
      val stringArray = line.split(",")
      val feature = stringArray(0).drop(1).toInt
      val weight = stringArray(1).dropRight(1).toDouble
      (feature, weight)
    })

    val testBritney = modelBritneyData.collect.toMap
    val brvalBritney = sc.broadcast(testBritney)

    val methodType = args.method()

    val result = textFile.map(line => {
      val trainingInstanceArray = line.split(" ")
      val docid = trainingInstanceArray(0)
      val label = trainingInstanceArray(1)

      var scoreX = 0d
      var scoreY = 0d
      var scoreBritney = 0d
      var finalScore = 0d

      for (x <- 2 until trainingInstanceArray.length) {
        val feature = trainingInstanceArray(x).toInt
        // x
        if (brvalX.value.contains(feature)) {
          scoreX += brvalX.value(feature)
        }
        // y
        if (brvalY.value.contains(feature)) {
          scoreY += brvalY.value(feature)
        }
        // Britney
        if (brvalBritney.value.contains(feature)) {
          scoreBritney += brvalBritney.value(feature)
        }
      }

      if (methodType == "average") {
        finalScore = (scoreX + scoreY + scoreBritney) / 3
      }
      else if (methodType == "vote") {
        var xVote = 0
        var yVote = 0
        var britneyVote = 0
        if (scoreX > 0) xVote += 1 else xVote -= 1
        if (scoreY > 0) yVote += 1 else yVote -= 1
        if (scoreBritney > 0) britneyVote += 1 else britneyVote -= 1
        finalScore = xVote + yVote + britneyVote
      }
//      for (x <- 2 until trainingInstanceArray.length) {
//        val feature = trainingInstanceArray(x).toInt
//        //        if (w.contains(feature)) spamminessScore += w(feature)
//      }

      //      val featuresString = trainingInstanceArray.slice(2, trainingInstanceArray.length)
      //      val features = featuresString.map(_.toInt)


      //      features.foreach(f => if (w.contains(f)) spamminessScore += w(f))

      if (finalScore > 0)
        (docid, label, finalScore, "spam")
      else
        (docid, label, finalScore, "ham")
    })

    result.saveAsTextFile(args.output())


  }
}