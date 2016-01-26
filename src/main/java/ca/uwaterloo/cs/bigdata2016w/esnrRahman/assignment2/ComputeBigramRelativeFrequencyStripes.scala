package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment2

import ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment2.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.rogach.scallop._

import collection.mutable.HashMap

class ConfStripes(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  //  def calculateRelFreq(iter: Iterator[((String, String), Int)]): Iterator[((String, String), Float)] = {
  //    var x: Int = -1
  //    iter.map { case ((firstWord, secondWord), count) => {
  //      if (secondWord == "*") {
  //        x = count
  //        ((firstWord, secondWord), count.toFloat)
  //      } else {
  //        ((firstWord, secondWord), count.toFloat / x)
  //      }
  //    }
  //    }
  //  }

  class CustomPartitioner(val numPartitions: Int)
    extends Partitioner {

    def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      (k.hashCode & Integer.MAX_VALUE) % numPartitions
    }
  }

  def main(argv: Array[String]) {
    val args = new ConfStripes(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Use in-mapper combining: " + args.imc())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val stripes = new HashMap[String, HashMap[String, Float]]() {
          override def default(key: String) = new HashMap[String, Float]()
        }

        val bigramPairs = if (tokens.length > 1) tokens.sliding(2).map(p => (p(0), p(1))).toList else List()

        bigramPairs.foreach { bigram =>
          val firstWord: String = bigram._1
          val secondWord: String = bigram._2

          if (stripes.contains(firstWord)) {
            val stripe = stripes(firstWord)
            if (stripe.contains(secondWord)) {
              stripe.put(secondWord, stripe(secondWord) + 1.0f)
            }
            else {
              stripe.put(secondWord, 1.0f)
            }
          }
          else {
            val stripe = new HashMap[String, Float]()
            stripe.put(secondWord, 1.0f)
            stripes.put(firstWord, stripe)
          }
        }
        stripes.iterator
      })
      //      .partitionBy(new CustomPartitioner(args.reducers()))
      //      .reduceByKey((str, innerMap) => {

      //      })
      //      .reduceByKey(new CustomPartitioner(args.reducers()), _ + _)
      .repartitionAndSortWithinPartitions(new CustomPartitioner(args.reducers()))
      //      .reduceByKey((val1, val2) => {
      //      val2.foreach { (secondWord) =>
      //        if (val1.contains(secondWord)) {
      //          val1.put(secondWord, val1(secondWord) + count)
      //        }
      //        else {
      //          val1.put(secondWord, count)
      //        }
      //      }
      //      val1
      //    })

      //      .mapPartitions(calculateRelFreq)
      .saveAsTextFile(args.output())

  }

}
