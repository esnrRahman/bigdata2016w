package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input dir", required = true)
  val date = opt[String](descr = "date param", required = true)
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.date())

    val conf = new SparkConf().setAppName("SQL Query 4")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.date()

    val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
    val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
    val custTextFile = sc.textFile(args.input() + "/customer.tbl")
    val nationTextFile = sc.textFile(args.input() + "/nation.tbl")


    val nationNames = nationTextFile
      .map(line => {
        val nationTable = line.split("\\|")
        val nNationKey = nationTable(0)
        val nNationName = nationTable(1)
        (nNationKey, nNationName)
      })

    val nationNameList = sc.broadcast(nationNames.collectAsMap())

    val custKeys = custTextFile
      .map(line => {
        val custTable = line.split("\\|")
        val cCustKey = custTable(0)
        val cNationKey = custTable(3)
        (cCustKey, cNationKey)
      })

    val custKeyList = sc.broadcast(custKeys.collectAsMap())

    val orderCustKeys = ordersTextFile
      .map(line => {
        val orderTable = line.split("\\|")
        val oOrderKey = orderTable(0)
        val oCustKey = orderTable(1)
        (oOrderKey, oCustKey)
      })
      .filter(tuple => custKeyList.value(tuple._2).nonEmpty)

    val lineItems = lineItemTextFile
      .map(line => {
        val ltOrderTable = line.split("\\|")
        val ltOrderKey = ltOrderTable(0)
        val ltShipDate = ltOrderTable(10)
        (ltOrderKey, ltShipDate)
      })
      // Do date filter
      .filter(tuple => tuple._2.startsWith(queriedShipDate))
      // Do a cogroup join. Result is (joinedOrderKey, (ShipDate, CustKey))
      .cogroup(orderCustKeys)
      // This filter removes all elements where ltOrderKey != oOrderKey
      .filter(tuple => tuple._2._1.nonEmpty && tuple._2._2.nonEmpty)

      //    val finalTable = lineItems.collect()
      //    println("EHSAN !!!" + finalTable.length)
      //    for (i <- finalTable) {
      //      println("(" + i._1 + "," + i._1 + ")")
      //    }

      // Rearrange to get (custKey, shipDate occurrence)
      .map(tuple => (tuple._2._2.toList.head, tuple._2._1.size)) // OK here
      // Refer to custKeyList to get (cNationKey, occurrence #)
      .map(tuple => (custKeyList.value(tuple._1), tuple._2))
      .map(tuple => (tuple._1, nationNameList.value.get(tuple._1), tuple._2))
      .map(threeTuple => ((threeTuple._1, threeTuple._2), threeTuple._3))
      .reduceByKey(_ + _)
      //        .sortByKey(_._1._1)
      .map(tuple => (Integer.parseInt(tuple._1._1), (tuple._1._2, tuple._2)))
      .sortByKey()

    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }

    val finalTable = lineItems.collect()
    for (i <- finalTable) {
      println("(" + i._1 + "," + show(i._2._1) + "," + i._2._2 + ")")
    }
  }

}
