package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input dir", required = true)
  val date = opt[String](descr = "date param", required = true)
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.date())

    val conf = new SparkConf().setAppName("SQL Query 1")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.date()

    val lineItemtextFile = sc.textFile(args.input() + "/lineitem.tbl")
    val orderTextFile = sc.textFile(args.input() + "/orders.tbl")

    val shipDates = lineItemtextFile
      .flatMap(line => {
        val lineItemTable = line.split("\\|")
        val orderKey = lineItemTable(0)
        var shipDate = lineItemTable(10)
        val dateFormatLength = queriedShipDate.split("\\-").length
        // Check date format
        if (dateFormatLength == 2) {
          shipDate = shipDate.dropRight(3)
        } else if (dateFormatLength == 1) {
          shipDate = shipDate.dropRight(6)
        }
        if (shipDate == queriedShipDate) List((orderKey,shipDate)) else List()
      })

//    for (i <- shipDates) {
//      println("(" + i._2 + "," + i._1 + ")")
//    }

    val clerkNumber = orderTextFile
      .flatMap(line => {
        val orderTable = line.split("\\|")
        val orderKey = orderTable(0)
        val clerkNumber = orderTable(6)
        List((orderKey, clerkNumber))
      })

//    for (i <- clerkNumber) {
//      println("(" + i._2 + "," + i._1 + ")")
//    }

    val combinedTable = shipDates.cogroup(clerkNumber)
        .flatMap(tuple => {
          val orderKey = tuple._1
          val tempList = tuple._2._2.toList
//          if (tempList.size == 1) List() else List((orderKey, tempList.head))
          List((orderKey, tempList))
        })
        .sortByKey()

    // Print Answer
    for (i <- combinedTable) {
      println("(" + i._1 + "," + i._2 + ")")
    }
  }
}
