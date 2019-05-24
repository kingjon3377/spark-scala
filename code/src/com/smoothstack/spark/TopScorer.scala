package com.smoothstack.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object TopScorer {
  case class Player(name: String, id: Int)
  case class Shot(gameId: Int, matchup: String, homeGame: Boolean, wonGame: Boolean,
      finalMargin: Int, shotNumber: Int, period: Integer, gameClock: Double,
      shotClock: Double, dribbles: Int, touchTime: Double, shotDistance: Double,
      shotType: Int, shotMade: Boolean, closestDefender: Player,
      closestDefenderDistance: Double, playPoints: Int, player: Player)
  case class ShotSummary(player: Player, average: Double)

  def parseHomeGame(input: String): Option[Boolean] = {
    input match {
      case "A" => return Some(false)
      case "H" => return Some(true)
      case _ => return None
    }
  }

  def parseWonGame(input: String): Option[Boolean] = {
    input match {
      case "W" => return Some(true)
      case "L" => return Some(false)
      case _ => return None
    }
  }

  def parseClock(field: String): Double = {
    val split = field.split(':');
    if (split.length > 1) {
      return split(0).toDouble + split(1).toDouble / 60
    } else {
      return split(0).toDouble
    }
  }

  def parseShotClock(field: String): Double = field match {
    case "" => 0.0
    case _ => field.toDouble
  }

  def mapper(line: String): Option[Shot] = {
    // CSV-parsing regex taken from https://stackoverflow.com/a/13336039
    val fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    if (fields(0) == "GAME_ID") {
      return None
    }
    val homeGameRaw = parseHomeGame(fields(2))
    if (homeGameRaw.isEmpty) {
        println("Neither home game nor away game")
        return None
    }
    val homeGame: Boolean = homeGameRaw.get

    val wonGameRaw = parseWonGame(fields(3))
    if (wonGameRaw.isEmpty) {
      println("Neither won nor lost game")
      return None
    }
    val wonGame: Boolean = wonGameRaw.get

    val shotMadeRaw = fields(13) match {
      case "made" => Some(true)
      case "missed" => Some(false)
      case _ => None
    }
    if (shotMadeRaw.isEmpty) {
      println("Neither made nor missed shot")
      return None
    }
    val shotMade: Boolean = shotMadeRaw.get

    return Some(Shot(fields(0).toInt, fields(1), homeGame, wonGame, fields(4).toInt,
        fields(5).toInt, fields(6).toInt, parseClock(fields(7)), parseShotClock(fields(8)),
        fields(9).toInt, fields(10).toDouble, fields(11).toDouble, fields(12).toInt,
        shotMade, Player(fields(14), fields(15).toInt), fields(16).toDouble, fields(18).toInt,
        Player(fields(19), fields(20).toInt)))
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("TopScorer").master("local[*]").getOrCreate()
    val lines = spark.sparkContext.textFile("../nba-shot-logs/shot_logs.csv")
    import spark.implicits._
    val shots = lines.flatMap(mapper).toDS

    println("Top Ten Scorers (ordering by defender distance):")

    shots.filter($"shotMade").map(shot => ShotSummary(shot.player, 
        shot.playPoints.toDouble / shot.closestDefenderDistance)).
      groupBy($"player").mean("average").sort($"avg(average)".desc).
      limit(10).map(_.getStruct(0).getAs[String](0)).foreach(println(_))
  }
}
