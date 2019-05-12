package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the day of maximum precipitation by weather station */
object MaxPrecip {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val day = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, day, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../SparkScala/1800.csv")
    
    // Convert to (stationID, day, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but PRCP entries
    val maxPrecip = parsedLines.filter(x => x._3 == "PRCP")
    
    // Convert to (stationID, (day, max precipitation))
    val stationPrecip = maxPrecip.map(x => (x._1, (x._2, x._4.toFloat)))
    
    // Reduce by stationID retaining the maximum temperature found
    val maxPrecipByStation = stationPrecip.reduceByKey( (x,y) => {
      if (x._2 >= y._2) {
        x
      } else {
        y
      }
    });
    
    val maxPrecipDays = maxPrecipByStation.mapValues(x => x._1)
    
    // Collect, format, and print the results
    val results = maxPrecipDays.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val day = result._2
       println(s"$station day of most precipitation: $day")
    }
      
  }
}