package com.bingo.offlineRecom

import java.util.Properties

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import java.util.Date
import java.time.Duration

object recomALS {
  case class Recommend(uid: Int, mid: Int, rating: Double)

  case class SimMovie(mid: Int, simId: Int, similarity: Double)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\winutils")
    val conf = new SparkConf().setAppName("JdbcOperation")//.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val properties = new Properties()
//    properties.put("user","bingodatabase%bingo")
//    properties.put("password","Brokendouban123")
    properties.put("user","root")
    properties.put("password","root")
//    val url = "jdbc:mysql://bingodatabase.mysqldb.chinacloudapi.cn:3306/test4"
    val url = "jdbc:mysql://192.168.132.2:3306/brokendouban"
    val ratingDF = sqlContext.read.jdbc(url,"rating",properties)
    ratingDF.printSchema()
    println(ratingDF.count())
    ratingDF.show()
    def parseRating(row: Row): Rating = {
      Rating(row.getInt(1), row.getInt(2), row.getDouble(3))
    }

    val ratingRDD = ratingDF.rdd.cache()

    val ratings = ratingRDD.map(parseRating)
    //   val ratings = ratingRDD.foreach(printRating)
    //    println(ratingRDD)
    //ALS
    // val movieIds = ratings.map(_.product).distinct().collect

    val splits=ratings.randomSplit(Array(0.8,0.2), 0L)
    val trainingSet=splits(0).cache()
    val testSet=splits(1).cache()
    trainingSet.count()
    testSet.count()
    //val model=(new ALS().setRank(20).setIterations(10).run(trainingSet))
    //val recomForTopUser=model.recommendProducts(4,5).foreach(println)

    val model = parameterAdjust.evaluateAllParameter(trainingSet,testSet,Array(5, 10, 15, 20, 25), Array(5, 10, 15), Array(0.05, 0.1, 1, 5, 10.0))

    // model.save(sc,"file:///d://alsModel")

    //    model.recommendProductsForUsers(5).foreach(println)
    model.productFeatures.foreach(println)

    import sqlContext.implicits._
    val recommendDF = model.recommendProductsForUsers(5).flatMap(_._2).map(line => Recommend(line.user, line.product, 0)).toDF()
    recommendDF.show()
    println(recommendDF.count())

    recommendDF.write.jdbc(url,"recommend",properties)

    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      if ((vec1.norm2() * vec2.norm2()) != 0){
        return vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
      }
      return -1.0
    }

    val movieIds  = model.productFeatures.map(_._1).distinct().collect()

    val itemId = movieIds(0)
    // 获取该物品的隐因子向量
    val itemFactor = model.productFeatures.lookup(itemId).head
    // 将该向量转换为jblas矩阵类型
    val itemVector = new DoubleMatrix(itemFactor)
    // 计算该电影与其他电影的相似度
    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    // 获取与电影567最相似的10部电影
    var sortedSims = sims.sortBy(_._2,false).take(1).map(each => (itemId, each._1, each._2))
    // 打印结果
    println(sortedSims.mkString("\n"))

    for(movieId <- movieIds){
      val itemId = movieId
      // 获取该物品的隐因子向量
      val itemFactor = model.productFeatures.lookup(itemId).head
      // 将该向量转换为jblas矩阵类型
      val itemVector = new DoubleMatrix(itemFactor)
      // 计算电影的相似度
      val sims = model.productFeatures.map{ case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (id, sim)
      }
      // 获取与电影567最相似的10部电影 val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
      sortedSims = sortedSims.union(sims.sortBy(_._2,false).take(10).map(each => (movieId, each._1, each._2)))
      //sortedSims = sortedSims.union(sims.top(6)(Ordering.by[(Int, Double), Double]))
      // 打印结果

    }

    import sqlContext.implicits._
    val simMovieDF = sc.parallelize(sortedSims.map(each => SimMovie(each._1, each._2, each._3))).toDF()

    //simMovieDF.filter(df() = )
    simMovieDF.show()
    simMovieDF.write.jdbc(url,"simMovies",properties)
    //simMovieDF.write.format("com.databricks.spark.csv").save("file:///d://sim.csv")
  }



}
