package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment2

import ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment2.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  // def wcIter(iter: Iterator[String]): Iterator[(String, Int)] = {
  //   val counts = new HashMap[String, Int]() { override def default(key: String) = 0 }

  //   iter.flatMap(line => tokenize(line))
  //     .foreach { t => counts.put(t, counts(t) + 1) }

  //   counts.iterator
  // }

  def calculateRelFreq(iter: Iterator[((String, String), Int)]): Iterator[((String, String), Float)] = {
    var x: Int = -1
    iter.map { case ((firstWord, secondWord), count) => {
      if (secondWord == "*") {
        x = count
        ((firstWord, secondWord), count.toFloat)
      } else {
        ((firstWord, secondWord), count.toFloat / x)
      }
    }
    }
  }

  class CustomPartitioner(val numPartitions: Int)
    extends Partitioner {

    def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[(String, String)]
      //      k * partitions / elements
      (k._1.hashCode & Integer.MAX_VALUE) % numPartitions
    }
  }


  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Use in-mapper combining: " + args.imc())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    // if (!args.imc()) {
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)

        //          val wordCount = tokens
        //            .map(word => (word, 1))
        //            .reduceByKey(_ + _)
        // .saveAsTextFile(args.output())

        if (tokens.length > 1) tokens.sliding(2).map(p => (p(0), p(1))).toList else List()

        //            .reduceByKey(_ + _)

        //          wordCount.union(bigramCount)

      })
      .flatMap(pair => {
        val firstWord = (pair._1, "*")
        //            println(firstWord)
        (firstWord, 1) :: List((pair, 1))
      })
      .reduceByKey(new CustomPartitioner(args.reducers()), _ + _)
      .repartitionAndSortWithinPartitions(new CustomPartitioner(args.reducers()))
      .mapPartitions(calculateRelFreq)
      .saveAsTextFile(args.output())

    // } else {
    //   textFile
    //     .mapPartitions(wcIter)
    //     .reduceByKey(_ + _)
    //     .saveAsTextFile(args.output())
    // }
  }
}
