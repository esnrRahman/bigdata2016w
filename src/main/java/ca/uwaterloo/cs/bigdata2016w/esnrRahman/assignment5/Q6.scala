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

        val sumDiscPrice = ltExtendedPrice.toDouble * (1.0d - ltDiscount.toDouble)
        val sumCharge = ltExtendedPrice.toDouble * (1.0d - ltDiscount.toDouble) * (1.0d + ltTax.toDouble)

        (ltReturnFlag, ltLineStatus, ltQuantity, ltExtendedPrice, sumDiscPrice, sumCharge, 1.0d, ltDiscount.toDouble, ltShipDate)
      })
      .filter(set => set._9.startsWith(queriedShipDate))
      .map(set => ((set._1, set._2), (set._3.toDouble, set._4.toDouble, set._5, set._6, set._7, set._8)))
//          .reduceByKey((firstSet, secondSet) =>
//            (firstSet._1 + secondSet._2,
//              firstSet._2 + secondSet._2,
//              firstSet._3, + secondSet._3,
//              firstSet._4 + secondSet._4,
//              firstSet._5, + secondSet._5,
//              firstSet._6, + secondSet._6
//              )
//          ).collect().toList

    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }

    val finalTable = lineItems.collect()
    for (i <- finalTable) {
      println("XXXXXXXXXX")
      println("(" + i._1._1 + "," + i._1._2 + "," + i._2._1 + "," + i._2._2 + "," + i._2._3+ "," + i._2._4+ "," + i._2._5 + "," + i._2._6 + ")")
      println("XXXXXXXXXX")
    }
  }

}
