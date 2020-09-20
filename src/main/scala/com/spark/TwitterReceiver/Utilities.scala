package com.spark.TwitterReceiver

object Utilities {

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
  def setTwitterConn() = {
    import scala.io.Source

    /*E:\\BigData\\Scala\\Spark\\twitter.txt"*/
    for (line <- Source.fromFile("twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
        println(fields(0),fields(1))
      }

    }
  }

}
