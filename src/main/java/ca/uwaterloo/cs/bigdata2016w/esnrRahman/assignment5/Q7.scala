package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input dir", required = true)
  val date = opt[String](descr = "date param", required = true)
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.date())

    val conf = new SparkConf().setAppName("SQL Query 7")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.date()

    val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
    val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
    val custTextFile = sc.textFile(args.input() + "/customer.tbl")

    val custNames = custTextFile
      .map(line => {
        val custTable = line.split("\\|")
        val cCustKey = custTable(0)
        val cCustName = custTable(1)
        (cCustKey, cCustName)
      })

    val custNameList = sc.broadcast(custNames.collectAsMap())

    // Filter and process Order Table
    val orderCustKeys = ordersTextFile
      .map(line => {
        val orderTable = line.split("\\|")
        val oOrderKey = orderTable(0)
        val oCustKey = orderTable(1)
        val oOrderDate = orderTable(4)
        val oOrderShippingPriority = orderTable(7)
        (oOrderKey, oCustKey, oOrderDate, oOrderShippingPriority)
      })
      .filter(set => {
        val queriedDateFormatLength = queriedShipDate.split("\\-").length
        var orderDate = set._3
        if (queriedDateFormatLength == 2) {
          orderDate = orderDate.dropRight(3)
        } else if (queriedDateFormatLength == 1) {
          orderDate = orderDate.dropRight(6)
        }
        val queriedDateNum = queriedShipDate.split("\\-").mkString("").toDouble
        val orderDateNum = orderDate.split("\\-").mkString("").toDouble
        if (orderDateNum < queriedDateNum) true else false
      })
      .filter(set => custNameList.value(set._2).nonEmpty)
      // Rearrange in the form of (oOrderKey, (cCustName, oOrderDate, oShippingPriority))
      .map(set => (set._1, (custNameList.value(set._2), set._3, set._4)))

    val lineItems = lineItemTextFile
      .map(line => {
        val ltOrderTable = line.split("\\|")
        val ltOrderKey = ltOrderTable(0)
        val ltExtendedPrice = ltOrderTable(5)
        val ltDiscount = ltOrderTable(6)
        val ltShipDate = ltOrderTable(10)

        val discPrice = ltExtendedPrice.toDouble * (1.0d - ltDiscount.toDouble)

        (ltOrderKey, discPrice, ltShipDate)
      })
      .filter(set => {
        val queriedDateFormatLength = queriedShipDate.split("\\-").length
        var shipDate = set._3
        if (queriedDateFormatLength == 2) {
          shipDate = shipDate.dropRight(3)
        } else if (queriedDateFormatLength == 1) {
          shipDate = shipDate.dropRight(6)
        }
        val queriedDateNum = queriedShipDate.split("\\-").mkString("").toDouble
        val shipDateNum = shipDate.split("\\-").mkString("").toDouble
        if (shipDateNum > queriedDateNum) true else false
      })
      // Rearrange in the form of (ltOrderKey, (discPrice, ShippingDate))
      .map(set => (set._1, (set._2, set._3)))

    val joinedTable = lineItems.cogroup(orderCustKeys)
      // This filter removes all elements where ltOrderKey != oOrderKey
      // Arrangement is in the form (joinedOrderKey,
      //                                  ( (discPrice, ShippingDate),
      //                                    (cCustName, oOrderDate, oShippingPriority) ))
      // set._1 => joinedOrderKey
      // set._2._1 => (discPrice, ShippingDate)
      // set._2._2 => (cCustName, oOrderDate, oShippingPriority)
      .filter(set => set._2._1.nonEmpty && set._2._2.nonEmpty)
      .map(set => {
        val orderKey = set._1
        val custName = set._2._2.toList.head._1
        val orderDate = set._2._2.toList.head._2
        val shippingPriority = set._2._2.toList.head._3
        val discPrice = set._2._1.toList.head._1
        val shippingDate = set._2._1.toList.head._2
        ((custName, orderKey, orderDate, shippingPriority), discPrice.toDouble)
      })
      .reduceByKey((val1, val2) => val1 + val2)
      .map(set => {
        val custName = set._1._1
        val orderKey = set._1._2
        val orderDate = set._1._3
        val shippingPriority = set._1._4
        val revenue = set._2
        ((custName, orderKey, orderDate, shippingPriority), revenue)
      })
      .groupByKey()
      .map(set => {
        ((set._1._1, set._1._2, set._1._3, set._1._4), set._2.map(revenue => revenue).sum)
      }).sortBy(set => set._2, false)


    val finalTable = joinedTable.take(10)
    for (i <- finalTable) {
      println("(" + i._1._1 + "," + i._1._2 + "," + i._2 + "," + i._1._3 + "," + i._1._4 + ")")
    }
  }

}
