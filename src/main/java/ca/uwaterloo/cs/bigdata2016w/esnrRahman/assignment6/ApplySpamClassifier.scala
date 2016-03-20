package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input file", required = true)
  val output = opt[String](descr = "output dir", required = true)
  val model = opt[String](descr = "model dir", required = true)
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

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
      // Getting stuck at the next line
      (feature, weight)
    })

    val test = modelData.collectAsMap()

    val result = textFile.map(line => {
      val trainingInstanceArray = line.split(" ")
      val docid = trainingInstanceArray(0)
      val label = trainingInstanceArray(1)

      var spamminessScore = 0d

//      for (x <- trainingInstanceArray) {
//        if (test.contains(x)) spamminessScore += test(x)
//      }

      trainingInstanceArray.foreach(f => if (test.contains(f.toInt)) spamminessScore += test(f.toInt))

      if (spamminessScore > 0)
        (docid, label, spamminessScore, "spam")
      else
        (docid, label, spamminessScore, "ham")
    })

    result.saveAsTextFile(args.output())


  }
}