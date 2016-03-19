package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

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
  def spamminess(features: Array[Int]): Double = {
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
      val trainingInstance = line.split(" ")
      val docid = trainingInstance(0)
      val label = trainingInstance(1)

      val isSpam = if (label == "spam") 1 else 0

      trainingInstance.toBuffer.remove(0, 1)

      (0, (docid, isSpam, trainingInstance))
    })
      .groupByKey(1)
      .map(pair => {
        // Then run the trainer...

        // This is the main learner:
        val delta = 0.002

        // For each instance...
        val isSpam = pair._2.iterator.next()._2
        // label
        val featuresString = pair._2.iterator.next()._3 // feature vector of the training instance

        val features = featuresString.map(_.toInt)

        // Update the weights as follows:
        val score = spamminess(features)
        val prob = 1.0 / (1 + Math.exp(-score))
        features.foreach(f => {
          if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
          } else {
            val result = (isSpam - prob) * delta
            w += (f -> result)
          }
        })
        w
      })

    trained.saveAsTextFile(args.model())


  }
}