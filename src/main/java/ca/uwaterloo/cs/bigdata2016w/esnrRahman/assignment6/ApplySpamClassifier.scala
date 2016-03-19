package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.Buffer

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(output, model)
  val output = opt[String](descr = "input file", required = true)
  val model = opt[String](descr = "output dir", required = true)
}

object ApplySpamClassifier {
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
    val args = new Conf2(argv)

    log.info("Input path" + args.output())
    log.info("Output dir" + args.model())

    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.output())

    val trained = textFile.map(line => {
      val trainingInstanceArray = line.split(" ")
      val docid = trainingInstanceArray(0)
      val label = trainingInstanceArray(1)

      val features = trainingInstanceArray.toBuffer
      features -= docid
      features -= label

      (0, (docid, label, features))
    })
      .groupByKey(1)
      .map(pair => {
        // Then run the trainer...

        // This is the main learner:
        val delta = 0.002

        val pairList = pair._2.toList

        pairList.foreach(tuple => {
          // For each instance...
          val docId = tuple._1

          val label = tuple._2
          // label
          val featuresString = tuple._3 // feature vector of the training instance

          val features = featuresString.map(_.toInt)

          val score = spamminess(features)

          if (score > 0)
            (docId, label, score, "spam")
          else
            (docId, label, score, "ham")
        })
      })

    trained.saveAsTextFile(args.model())


  }
}