package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input dir", required = true)
  val date = opt[String](descr = "date param", required = true)
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.date())

    val conf = new SparkConf().setAppName("SQL Query 1")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.date()

    val textFile = sc.textFile(args.input() + "/lineitem.tbl")

    val numberOfItems = textFile
      .flatMap(line => {
        var shipDate = line.split("\\|")(10)
        val dateFormatLength = queriedShipDate.split("\\-").length
        // Check date format
        if (dateFormatLength == 2) {
          shipDate = shipDate.dropRight(3)
        } else if (dateFormatLength == 1) {
          shipDate = shipDate.dropRight(6)
        }
        if (shipDate == queriedShipDate) List(shipDate) else List()
      })
    println("ANSWER=" + numberOfItems.count())
  }
}
