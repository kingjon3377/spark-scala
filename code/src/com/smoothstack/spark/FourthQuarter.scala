package com.smoothstack.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object FourthQuarter {
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
      return split(0).toDouble * 60 + split(1).toDouble
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
    val spark = SparkSession.builder.appName("PostShooters").master("local[*]").getOrCreate()
    val lines = spark.sparkContext.textFile("../nba-shot-logs/shot_logs.csv")
    import spark.implicits._
    val shots = lines.flatMap(mapper).toDS

    println("Top Ten Fourth-Quarter Outside Shooters (by points per shot from 5+ ft out):")

    import org.apache.spark.sql.functions._
    shots.filter(shot => shot.shotDistance > 5.0).
      filter(shot => shot.gameClock >= 36.0 && shot.gameClock < 48.0).groupBy($"player").
      agg(count("shotClock"), sum("playPoints")).flatMap(group => { 
        if (group.getAs[Long](1) < 100) {
          None
        } else {
          val player = group.getStruct(0)
          Some(ShotSummary(Player(player.getAs(0), player.getAs(1)), 
              group.getAs[Long](2).toDouble / group.getAs[Long](1).toDouble))
        }
      }).sort($"average".desc).limit(10).map(shot => shot.player.name).
      foreach(name => println(name))
  }
}
