package org.basic.example

import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc}

object TwitterHashtag {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class TweetContent(content:String)

  val spark = SparkSession
    .builder
    .appName("Twitter Hashtag")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/AllTweets.csv")

  def main(args: Array[String]){

    val resultDs = countPopularHashtags(df)

    println("Top 20 popular hashtags of the AllTweets Dataset")

    resultDs.show(truncate = false)

//    resultDs
//      .write
//      .option("header", "true")
//      .save("data/hashtags.csv")
  }

  def countPopularHashtags(df:DataFrame): DataFrame ={

    val tweetContentDf = df.select("text").withColumnRenamed("text", "content").as[TweetContent]

    val contentDs = tweetContentDf.flatMap(r => if (r.content != null) r.content.split(" ") else Array(" "))

    val hashtagDs = contentDs.select("value").where(col("value") like "#%")

    val resultDs = hashtagDs.groupBy("value").count().orderBy(desc("count"))

    resultDs
  }

}
