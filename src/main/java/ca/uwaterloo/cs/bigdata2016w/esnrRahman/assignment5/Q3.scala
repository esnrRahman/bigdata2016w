package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input dir", required = true)
  val date = opt[String](descr = "date param", required = true)
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.date())

    val conf = new SparkConf().setAppName("SQL Query 3")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.date()

    val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
    val partTextFile = sc.textFile(args.input() + "/orders.tbl")
    val supplierTextFile = sc.textFile(args.input() + "/supplier.tbl")

    val partNames = partTextFile
      .map(line => {
        val partTable = line.split("\\|")
        val partKey = partTable(0)
        val partName = partTable(1)
        (partKey, partName)
      })

    val partsList = sc.broadcast(partNames.collectAsMap())

    val supplierNames = supplierTextFile
      .map(line => {
        val supplierTable = line.split("\\|")
        val supplierKey = supplierTable(0)
        val supplierName = supplierTable(1)
        (supplierKey, supplierName)
      })

    val supplierList = sc.broadcast(supplierNames.collectAsMap())

    val joinedTable = lineItemTextFile
      .flatMap(line => {
        val lineItemTable = line.split("\\|")
        val orderKey = lineItemTable(0)
        val partKey = lineItemTable(1)
        val supplierKey = lineItemTable(2)
        var shipDate = lineItemTable(10)
        val dateFormatLength = queriedShipDate.split("\\-").length
        val partName = partsList.value.get(partKey)
        val supplierName = supplierList.value.get(supplierKey)
        // Check date format
        if (dateFormatLength == 2) {
          shipDate = shipDate.dropRight(3)
        } else if (dateFormatLength == 1) {
          shipDate = shipDate.dropRight(6)
        }
        if ((shipDate == queriedShipDate) && (partName != null) && (supplierName != null)) List((orderKey, (partName, supplierName))) else List()
      })

    // Print Answer
    for (i <- joinedTable.take(20)) {
      println("(" + i._1 + "," + i._2._1 + "," + i._2._2 + ")")
    }
  }
}
