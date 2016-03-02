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
        val nationKey = nationTable(0)
        val nationName = nationTable(1)
        (nationKey, nationName)
      })

    val nationList = sc.broadcast(nationNames.collectAsMap())

    val oCustKeys = ordersTextFile
      .flatMap(line => {
        val orderTable = line.split("\\|")
        val orderKey = orderTable(0)
        val custKey = orderTable(1)
        List((orderKey, custKey))
      })

    val oCustList = sc.broadcast(oCustKeys.collectAsMap())

    val lineItemCustKeys = lineItemTextFile
      .flatMap(line => {
        val lineItemTable = line.split("\\|")
        val orderKey = lineItemTable(0)
        var shipDate = lineItemTable(10)
        val dateFormatLength = queriedShipDate.split("\\-").length
        val custKey = oCustList.value.get(orderKey)
        if (dateFormatLength == 2) {
          shipDate = shipDate.dropRight(3)
        } else if (dateFormatLength == 1) {
          shipDate = shipDate.dropRight(6)
        }
        if ((shipDate == queriedShipDate) && (custKey != null)) List((custKey, orderKey)) else List()
      })

    val lineItemCustList = sc.broadcast(lineItemCustKeys.collectAsMap())

    val custKeys = custTextFile
      .flatMap(line => {
        val custTable = line.split("\\|")
        val custKey = custTable(0)
        val nationKey = custTable(3)
        val nationName = nationList.value.get(nationKey)
        val custKeyExists = lineItemCustList.value.contains(Some(custKey))
        if (nationName != null && custKeyExists) List((Integer.parseInt(nationKey), nationName)) else List()
      })
      .keyBy(x => (x._1, x._2))
      .groupByKey()
      .sortBy(x => x._1._1)

    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }

    // Print Answer
    val finalTable = custKeys.collect()
    for (i <- finalTable) {
//      println("**********")
//      println(i)
//      println("XXXXXXXXXX")
//      println(i._1)
//      println("===========")
//      println(i._2)
//      println("~~~~~~~~~~~")
      println("(" + i._1._1 + "," + show(i._1._2) + "," + i._2.count(x => true) + ")")
    }
  }
}
