package ca.uwaterloo.cs.bigdata2016w.esnrRahman.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input dir", required = true)
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("SQL Query 4")
    val sc = new SparkContext(conf)

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

    val custKeys = custTextFile
      .flatMap(line => {
        val custTable = line.split("\\|")
        val custKey = custTable(0)
        val nationKey = custTable(3)
        val nationName = nationList.value.get(nationKey).get
        if (nationName != null && (nationList.value.get(nationKey).get == "CANADA" || nationList.value.get(nationKey).get == "UNITED STATES")) List((custKey, nationName)) else List()
      })

//    for (i <- custKeys) {
//      println("(" + i._1 + "," + i._2 + ")")
//    }

    val custList = sc.broadcast(custKeys.collectAsMap())

    val orderKeys = ordersTextFile
      .flatMap(line => {
        val orderTable = line.split("\\|")
        val orderKey = orderTable(0)
        val custKey = orderTable(1)
        List((custKey, orderKey))
      })

    val lineItemDates = lineItemTextFile
      .flatMap(line => {
        val lineItemTable = line.split("\\|")
        val orderKey = lineItemTable(0)
        val shipDate = lineItemTable(10)
        List((orderKey, shipDate))
      })

    val orderCustomerJoinedTable = orderKeys.cogroup(custKeys)
      .flatMap(tuple => {
        val custKey = tuple._1
        val nationName = tuple._2._2
        val orderKey = tuple._2._1
        if (orderKey.isEmpty || nationName.isEmpty) List() else List((orderKey.mkString(" "), nationName.mkString(" ")))
      })


    val orderCustomerLineItemJoinedTable = orderCustomerJoinedTable.cogroup(lineItemDates)
      .flatMap(tuple => {
        val orderKey = tuple._1
        val nationName = tuple._2._1
        val shipDate = tuple._2._2
        if (nationName.isEmpty || nationName == null) {
          List()
        } else {
          List((nationName, shipDate))
        }
//        println("HERE 1 !!" + tuple._1)
//        println("HERE 2 !!" + tuple._2)
//        println("HERE 3 !!" + custList.value.get(tuple._1))
//        if (custList.value.get(tuple._1).get != null) {
//          val nationKeyNameTuple = custList.value.get(tuple._1).get
//          List((Integer.parseInt(nationKeyNameTuple._1), nationKeyNameTuple._2))
//        } else {
//          List()
//        }
      })
      .keyBy(x => (x._1, x._2))
      .groupByKey()
      .sortBy(x => x._1._1)

    def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
    }

    // Print Answer
    val finalTable = orderCustomerLineItemJoinedTable.collect()
    for (i <- finalTable) {
      //      println("**********")
      //      println(i)
      //      println("XXXXXXXXXX")
      //      println(i._1)
      //      println("===========")
      //      println(i._2)
      //      println("~~~~~~~~~~~")
      println("(" + i._1._1 + "," + i._1._2 + "," + i._2.count(x => true) + ")")
    }
  }
}
