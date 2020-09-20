package com.spark.TwitterReceiver
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter.TwitterUtils
import Utilities._
import org.apache.spark.sql.SparkSession

import java.util.regex.Pattern
import java.util.regex.Matcher

object TwitterR extends App{
  setTwitterConn()
  setupLogging()

  //val conf = new SparkConf().setAppName("Twitterread").setMaster("local[*]").set("spark.sql.warehouse.dir","file:///C:/tmp")
  val conf = new SparkConf().setAppName("Twitterread").setMaster("local[*]")
  val ssc = new StreamingContext(conf,Seconds(1))
  val filter = Array("urstrulyMahesh", "kchirutweets", "iamnagarjuna", "venkymama", "RGVzoomin")
  val tweets = TwitterUtils.createStream(ssc, None, filter)
  val text = tweets.map(x => {
      (Option(x.getUser()).map(_.getScreenName).orNull, Option(x.getUser()).map(_.getLocation).orNull)
      })

  text.foreachRDD((rdd,time) => {
    val spark = SparkSession.builder().appName("Twitterread").getOrCreate()
    import spark.implicits._

    val requestdataFrame = rdd.map(x => Record(x._1,x._2)).toDF()
    requestdataFrame.createOrReplaceTempView("twitter")

    val twitterdataFrame = spark.sqlContext.sql("select * from twitter")
    twitterdataFrame.show()

    twitterdataFrame.write.format("com.mongodb.spark.sql.DefaultSource").
      option("uri","mongodb://localhost:33553/movielens.twitter").mode("append").save()

  })

  /*val hashTweets = text.map(x => (x,1))
  val mostActive = hashTweets.reduceByKeyAndWindow((x,y) => x+y,(x,y) => x-y,Seconds(300),Seconds(5))*/
  /*text.print()*/
  /*val indTweets = text.flatMap(x => x.split(" "))
  val hashTweets = indTweets.filter(x => x.startsWith("#")).map(x => (x,1))
  val popularStar = hashTweets.reduceByKeyAndWindow((x,y) => x+y,(x,y) => x-y,Seconds(300),Seconds(5))
  val sortedR = popularStar.transform(x => x.sortBy(y => y._2, false))
  sortedR.print()*/
  //ssc.checkpoint("E:\\BigData\\Python\\Programs\\ScalaSpark\\checkout")
  ssc.checkpoint("hdfs://sandbox-hdp.hortonworks.com:8020/checkpoint")
  //ssc.checkpoint(".")
  ssc.start()

  ssc.awaitTermination()
}
/** Case class for converting RDD to DataFrame */
case class Record(screenName: String, location: String)