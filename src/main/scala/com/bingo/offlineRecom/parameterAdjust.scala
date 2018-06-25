package com.bingo.offlineRecom

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.jfree.data.category.DefaultCategoryDataset

object parameterAdjust {
  case class Recommend(uid: Int, mid: Int, rating: Double)

  case class SimMovie(mid: Int, simId: Int, similarity: Double)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\winutils")
    val conf = new SparkConf().setAppName("JdbcOperation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val properties = new Properties()
    //    properties.put("user","bingodatabase%bingo")
    //    properties.put("password","Brokendouban123")
    properties.put("user","root")
    properties.put("password","root")
    //    val url = "jdbc:mysql://bingodatabase.mysqldb.chinacloudapi.cn:3306/test4"
    val url = "jdbc:mysql://127.0.0.1:3306/brokendouban"
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

    val bestModel = trainvalidation(trainingSet, testSet)

  }

  def trainvalidation(trainDate: RDD[Rating], validationData: RDD[Rating]): MatrixFactorizationModel = {
    println("-----评估rank参数-----")
    evaluateParameter(trainDate, validationData, "rank", Array(5, 10 ,15, 20, 50 ,100), Array(10), Array(0.1))
    println("-----评估numIterations-----")
    evaluateParameter(trainDate, validationData, "numIterations", Array(10), Array(5, 10 ,15, 20, 25), Array(0.1))
    println("-----评估lambda-----")
    evaluateParameter(trainDate, validationData, "lambda", Array(10), Array(10), Array(0.05, 0.1, 1, 5 ,10.0))
    println("-----所有参数交叉评估找出最好的参数组合-----")
    val bestModel = evaluateAllParameter(trainDate, validationData, Array(5, 10, 15, 20, 25), Array(5, 10, 15, 20, 25), Array(0.05, 0.1, 1, 5, 10.0))
    return (bestModel)
  }

  def evaluateParameter(trainData: RDD[Rating], validationDate: RDD[Rating], evaluateParameter: String, rankArray: Array[Int], numIterationsArray: Array[Int], lambdaArray: Array[Double]) = {
    var dataBarChart = new DefaultCategoryDataset()
    var dataLineChart = new DefaultCategoryDataset()
    for (rank <- rankArray; numIterations <- numIterationsArray; lambda <- lambdaArray){
      val (rmse, time) = trainModel(trainData, validationDate, rank, numIterations, lambda)
      val parameterDate = evaluateParameter match{
        case "rank" =>rank;
        case "numIterations" => numIterations;
        case "lambda" => lambda
      }
      dataBarChart.addValue(rmse, evaluateParameter, parameterDate.toString())
      dataLineChart.addValue(time, "Time", parameterDate.toString())
    }
    Chart.plotBarLineChart("ALS evaluations " + evaluateParameter, evaluateParameter, "RMSE", 0.58, 5, "Time", dataBarChart, dataLineChart)
  }

  def trainModel(trainData: RDD[Rating], validationData: RDD[Rating], rank: Int, iterations: Int, lambda: Double): (Double,Double) = {
    val startTime = System.currentTimeMillis()
    val model = ALS.train(trainData, rank, iterations, lambda)
    val endTime = System.currentTimeMillis()
    val Rmse = computeRmse(model, validationData)
    val duration = endTime - startTime
    println(f"训练参数：rank:$rank%3d, iterations:$iterations%.2f ,lambda = $lambda%.2f 结果 Rmse=$Rmse%.2f" + "训练需要时间" + duration + "毫秒")
    (Rmse, duration)
  }

  def computeRmse(model: MatrixFactorizationModel, RatingRDD: RDD[Rating]): Double = {
    val num = RatingRDD.count()
    val predictedRDD = model.predict(RatingRDD.map(r => (r.user, r.product)))
    val predictedAndRatings = predictedRDD.map(p => ((p.user, p.product), p.rating)).join(RatingRDD.map(r => ((r.user, r.product), r.rating))).values
    math.sqrt(predictedAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / num)

  }

  def evaluateAllParameter(traingData: RDD[Rating], validationDate: RDD[Rating], rankArray: Array[Int], numIterationsArray: Array[Int], lambdaArray: Array[Double]): MatrixFactorizationModel = {
    val evaluatons = for(rank <- rankArray; numIterations <- numIterationsArray; lambda <- lambdaArray) yield {
      val (rmse, time) = trainModel(traingData, validationDate, rank, numIterations, lambda)
      (rank, numIterations, lambda, rmse)
    }
    val Eval = (evaluatons.sortBy(_._4))
    val BestEval = Eval(0)
    println("最佳mmodel参数：rank:" + BestEval._1 + ",iterations" + BestEval._2, BestEval._3)
    val bestModel = ALS.train(traingData, BestEval._1, BestEval._2, BestEval._3)
    (bestModel)
  }
}
