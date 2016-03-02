package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input dir", required = true)
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("SQL Query 5")
    val sc = new SparkContext(conf)

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
      .filter(tuple => tuple._2 == "CANADA" || tuple._2 == "UNITED STATES")

        for (i <- nationNames.collect())  {
          println("(" + i._1 + "," + i._2 + ")")
        }


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
        val ltShipDate = ltOrderTable(10).dropRight(3)
        (ltOrderKey, ltShipDate)
      })
      // Do a cogroup join. Result is (joinedOrderKey, (ShipDate, CustKey))
      .cogroup(orderCustKeys)
      // This filter removes all elements where ltOrderKey != oOrderKey
      .filter(tuple => tuple._2._1.nonEmpty && tuple._2._2.nonEmpty)

      //    val finalTable = lineItems.collect()
      //    println("EHSAN !!!" + finalTable.length)
      //    for (i <- finalTable) {
      //      println("(" + i._1 + "," + i._1 + ")")
      //    }

      // Rearrange to get (custKey, shipDates)
      .map(tuple => (tuple._2._2.toList.head, tuple._2._1)) // OK here
      // Refer to custKeyList to get (cNationKey, list of Dates)
      .map(tuple => (custKeyList.value(tuple._1), tuple._2))

      // Rearrange to make it (NationKey, Date)
      .map(tuple => {
      tuple._2.map(date => {
        (tuple._1, date)
      })
    })
      .flatMap(x => x)

      // Rearrange to make it (nNationName, Date)
      .map(tuple => ((nationNameList.value(tuple._1), tuple._2), 1))

    //      .map(threeTuple => ((threeTuple._1, threeTuple._2), threeTuple._3))
//          .reduceByKey(_ + _)
    //      .map(tuple => (Integer.parseInt(tuple._1._1), (tuple._1._2, tuple._2)))
    //      .sortByKey()

    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }

    val finalTable = lineItems.collect()
    for (i <- finalTable) {
      println("(" + i._1 + "," + i._2 + ")")
    }
  }

}
