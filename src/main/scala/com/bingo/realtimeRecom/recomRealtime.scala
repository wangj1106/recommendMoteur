package com.bingo.realtimeRecom

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext

object recomRealtime {
  case class Recommend(uid: Int, mid: Int, rating: Double)

  case class Rating(uid: Int, mid: Int, rating: Double)

  case class SimMovie(mid: Int, simId: Int, similarity: Double)



  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

    if(args.length != 4){
      println("paramter")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap


    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    //messages.map(_._2).count().print

    messages.filter(_._2.contains("::")).map(_._2.split("::")).foreachRDD{rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      val properties = new Properties()
      properties.put("user","bingodatabase%bingo")
      properties.put("password","Brokendouban123")
      val url = "jdbc:mysql://bingodatabase.mysqldb.chinacloudapi.cn:3306/test4"

      val simmoviesTab = sqlContext.read.jdbc(url,"simmovies",properties).createOrReplaceTempView("simmovies")

      import sqlContext.implicits._
      val ratingsTab = rdd.map(line => Rating(line(0).toInt, line(1).toInt, line(2).toInt)).toDF().createOrReplaceTempView("ratings")

      val temp1Tab = sqlContext.sql("select uid, max(rating) as rating from ratings GROUP BY uid ").createOrReplaceTempView("temp1")

      val maxRatingTab = sqlContext.sql("select ratings.uid, ratings.mid, temp1.rating from temp1 left join ratings on temp1.uid = ratings.uid and temp1.rating = ratings.rating").createOrReplaceTempView("maxRating")

      val temp2Tab = sqlContext.sql("select mid, max(similarity) as similarity from simmovies group by mid").createOrReplaceTempView("temp2")

      val simItemMoviesTab = sqlContext.sql("select simmovies.mid, simmovies.simId, temp2.similarity from temp2 left join simmovies on temp2.mid = simmovies.mid and temp2.similarity = simmovies.similarity").createOrReplaceTempView("simItemMovies")

      val recommendDF = sqlContext.sql("select maxRating.uid as uid, simItemMovies.simId as mid, maxRating.rating as rating from maxRating left join simItemMovies on maxRating.mid = simItemMovies.mid")

      recommendDF.show()

      recommendDF.write.mode("append").jdbc(url, "recommend", properties)


    }

    ssc.start()
    ssc.awaitTermination()
  }

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
}
