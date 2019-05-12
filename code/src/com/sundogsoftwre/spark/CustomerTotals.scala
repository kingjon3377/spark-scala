package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerTotals {
  def parseLine(line:String) = {
    val fields = line.split(",")
    val customer = fields(0)
    val item = fields(1)
    val price = fields(2).toFloat
    (customer, price)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CustomerTotals")
    val lines = sc.textFile("../SparkScala/customer-orders.csv")
    val parsedLines = lines.map(parseLine)
    val customerTotals = parsedLines.reduceByKey((x, y) => x + y)
    val sortedTotals = customerTotals.map(x => (x._2, x._1)).sortByKey()
    for (row <- sortedTotals) {
      val customerId = row._2
      val total = row._1
      println(f"Customer $customerId%s spent $total%.2f")
    }
  }
}