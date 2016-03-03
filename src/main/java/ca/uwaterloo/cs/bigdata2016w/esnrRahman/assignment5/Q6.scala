package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input dir", required = true)
  val date = opt[String](descr = "date param", required = true)
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Ship Date: " + args.date())

    val conf = new SparkConf().setAppName("SQL Query 4")
    val sc = new SparkContext(conf)
    val queriedShipDate = args.date()

    val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")


    val lineItems = lineItemTextFile
      .map(line => {
        val ltOrderTable = line.split("\\|")
        val ltQuantity = ltOrderTable(4)
        val ltExtendedPrice = ltOrderTable(5)
        val ltDiscount = ltOrderTable(6)
        val ltTax = ltOrderTable(6)
        val ltReturnFlag = ltOrderTable(8)
        val ltLineStatus = ltOrderTable(9)
        val ltShipDate = ltOrderTable(10)

        val sumDiscPrice = ltExtendedPrice * (1 - Integer.parseInt(ltDiscount))
        val sumCharge = ltExtendedPrice * (1 - Integer.parseInt(ltDiscount)) * (1 + Integer.parseInt(ltTax))

        (ltReturnFlag, ltLineStatus, ltQuantity, ltExtendedPrice, ltDiscount, ltShipDate)
      })
      .filter(set => set._6.startsWith(queriedShipDate))
        



    //      // Do date filter
    //      .filter(tuple => tuple._2.startsWith(queriedShipDate))
    //      // Do a cogroup join. Result is (joinedOrderKey, (ShipDate, CustKey))
    //      .cogroup(orderCustKeys)
    //      // This filter removes all elements where ltOrderKey != oOrderKey
    //      .filter(tuple => tuple._2._1.nonEmpty && tuple._2._2.nonEmpty)
    //
    //      //    val finalTable = lineItems.collect()
    //      //    println("EHSAN !!!" + finalTable.length)
    //      //    for (i <- finalTable) {
    //      //      println("(" + i._1 + "," + i._1 + ")")
    //      //    }
    //
    //      // Rearrange to get (custKey, shipDate occurrence)
    //      .map(tuple => (tuple._2._2.toList.head, tuple._2._1.size)) // OK here
    //      // Refer to custKeyList to get (cNationKey, occurrence #)
    //      .map(tuple => (custKeyList.value(tuple._1), tuple._2))
    //      .map(tuple => (tuple._1, nationNameList.value.get(tuple._1), tuple._2))
    //      .map(threeTuple => ((threeTuple._1, threeTuple._2), threeTuple._3))
    //      .reduceByKey(_ + _)
    //      //        .sortByKey(_._1._1)
    //      .map(tuple => (Integer.parseInt(tuple._1._1), (tuple._1._2, tuple._2)))
    //      .sortByKey()

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
