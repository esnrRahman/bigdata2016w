package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, dateparam)
  val input = opt[String](descr = "input dir", required = true)
  val dateparam = opt[String](descr = "date param", required = true)
  }

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.dateparam())

    val conf = new SparkConf().setAppName("SQL Query 1")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.dateparam()

    val textFile = sc.textFile(args.input() + "/lineitem.tbl")

    textFile
      .flatMap(line => {
        val shipDate = line.split("|")
        if (shipDate == queriedShipDate) shipDate.toList else List()
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(args.dateparam())
  }
}
