## 项目名称
电影推荐系统——烂豆瓣

## 项目目标
打开电影网站，脑袋就开始发大，动作片，爱情片，科幻片，中国的，欧美的，日韩的，到底哪一部最合自己的口味？茫茫影海，想要找到自己的“真爱”，谈何容易？我们知道你们找得辛苦，所以我们为你量身推荐电影！

## 项目任务
| 任务        | 具体任务   |  负责人  | 工作量  |
| :--------:   | -----  | ----  |   ----  |
|    数据集  | 负责完成收集电影、用户以及评分数据集，并对数据进行清洗，建立新的数据结构。建立并维护系统数据库。 |        |  |
|    推荐引擎  | 负责完成推荐系统，包括基于用户历史数据的离线推荐系统以及收集用户实时行为数据，进行精准的实时推荐。 |        |  |
|    API服务  | 负责完成基于烂豆瓣各产品，面向开发者的开放接口（API）服务。在这里，开发者可以接入烂豆瓣电影推荐的优质内容，以及基于各种兴趣的用户关系。 |        |  |
|    web应用  | 负责完成一个电影推荐web应用，利用开发接口API以及酷炫的web前端页面，实现用户与推荐系统的完美交互。 |        |  |

## 数据集
本系统所需要的电影数据，主要来源于IMDB，movielens, 以及豆瓣网站。本系统有两个数据集，第一个数据集是电影信息数据集，由IMDB网站提供电影的基本信息，包括电影的名称，年份，导演，演员，以及IMDB号。其中IMDB号为电影的唯一标识。通过IMDB号利用爬虫技术爬取豆瓣的电影图片。该数据集约20000条数据。第二个数据集是用户评分数据集，由Movielens提供了6000位用户，对5000多电影的评分。该数据集约600000条数据。
所涉及的技术是python爬虫（request框架），mysql数据库设计。

## 技术

![avatar](http://p35hkafb9.bkt.clouddn.com/%E6%95%B4%E4%BD%93%E6%9E%B6%E6%9E%84%E5%9B%BE.png)

### 离线推荐
1.MLlib的推荐算法工具
MLlib是Spark中用于机器学习的强大工具包。协同过滤推荐是MLlib提供的核心功能之一， org.apache.spark.mllib.recommendation中提供了3个用于协同过滤推荐的数据类型，即Rating、ALS和MatrixFactorizationModel。

| 类型        | 解释   |
| --------   | -----  |
| Rating |	Rating对象是一个用户、项目和评分的三元组。 |
| ALS |	ALS提供了求解带偏置矩阵分解的交替最小二乘算法（Alternating Least Squares，ALS）。 |
| MatrixFactorizationModel |	ALS求解矩阵分解返回的结果类型。 |

作为训练结果的MatrixFactorizationModel中提供了多种推荐操作。

| 方法        | 解释   |
| --------   | -----  |
| val productFeatures | 	RDD[（Int，Array[Double]）]：返回矩阵分解得的项目特征。    |
| val userFeatures |  	RDD[（Int，Array[Double]）]：返回矩阵分解得的用户特征。  |
| def predict      |	  RDD[Rating]：根据参数中需要预测的用户-项目，返回预测的评分结果。  |
| def predict       | 	预测用户user对项目product的评分。   |
| def recommendProducts  |  	为用户user推荐个数为num的商品。  |
| def recommendUsers   |  	为项目produoct推荐可能对其感兴趣的num个用户。   |

2.使用MLlib协同过滤实现离线电影推荐
（1）确定最佳的协同过滤模型参数
使用ALS算法求解矩阵分解时，需要设定3个参数：矩阵分解的秩rank、正则系数alpha和迭代次数numIters。为了确定最佳的模型参数，将数据集划分为3个部分：训练集、验证集和测试集。
训练集是用来训练多个协同过滤模型，验证集从中选择出均方误差最小的模型，测试集用来验证最佳模型的预测准确率。
<i class="icon-adjust"></i>步骤1　首先读取电影和评分的数据。

```scala
val sqlContext = new SQLContext(sc)
val properties = new Properties()
properties.put("user","root")
properties.put("password","root")
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

```

<i class="icon-adjust"></i>步骤2　利用timestamp将数据集分为训练集。

```scala
val splits=ratings.randomSplit(Array(0.8,0.2), 0L)
val trainingSet=splits(0).cache()
val testSet=splits(1).cache()
```

<i class="icon-adjust"></i>步骤3　定义函数计算均方误差RMSE。

```scala
Def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) : Double = {
val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
va lpredictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), x.rating)
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }
```

<i class="icon-adjust"></i>步骤4　使用不同的参数训练协同过滤模型，并且选择出RMSE最小的模型（为了简单起见，只从一个较小的参数范围选择：矩阵分解的秩从8~12中选择，正则系数从1.0~10.0中选择，迭代次数从10~20中选择，共计8个模型。读者可以根据实际需要调整选择范围）。

```scala
val ranks = List(8, 12)
val lambdas = List(1.0, 10.0)
valnumIters = List(10, 20)
varbestModel: Option[MatrixFactorizationModel] = None
varbestValidationRmse = Double.MaxValue
varbestRank = 0
varbestLambda = -1.0
varbestNumIter = -1
for (rank <- ranks; lambda <- lambdas; numIter<- numIters) {
val model = ALS.train(training, rank, numIter, lambda)
valvalidationRmse = computeRmse(model, validation)
if (validationRmse<bestValidationRmse) {
bestModel = Some(model)
bestValidationRmse = validationRmse
bestRank = rank
bestLambda = lambda
bestNumIter = numIter
      }
}
valtestRmse = computeRmse(bestModel.get, test)
println("The best model was trained with rank = " + bestRank + 
" and lambda = " + bestLambda+ 
", and numIter = " + bestNumIter + 
", and its RMSE on the test set is " + testRmse + ".")
```

<i class="icon-adjust"></i>步骤5　同时，还可以对比使用协同过滤算法和不使用协同过滤（例如，使用平均分来作为预测结果）能得到多大的预测效果提升。

```scala
val meanR = training.union(validation).map(_.rating).mean
val baseRmse = math.sqrt(test.map(x => (meanR - x.rating) * (meanR - x.rating)).mean)
val improvement = (baseRmse - testRmse) / baseRmse * 100
println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
```

（2）利用最佳模型进行电影推荐
得到了最佳的协同过滤模型后，可以使用该模型来为用户推荐前10的电影，并存储到数据库中。

```scala
import sqlContext.implicits._
    val recommendDF = model.recommendProductsForUsers(5).flatMap(_._2).map(line => Recommend(line.user, line.product, 0)).toDF()
    recommendDF.show()
    println(recommendDF.count())
    recommendDF.write.jdbc(url,"recommend",properties)
```

### 实时推荐
前期已经完成了推荐系统离线计算部分，主要是根据ALS、Itemcf进行推荐，这种离线的推荐在计算周期内推荐结果不发生改变，从而缺乏一定的个性化效果。个性化推荐则需要用户发生行为，并根据用户实时行为实时为其推送推荐结果。

![avatar](http://p35hkafb9.bkt.clouddn.com/%E5%AE%9E%E6%97%B6%E6%8E%A8%E8%8D%90%E6%9E%B6%E6%9E%84%E5%9B%BE.bmp)

（1）物品相似度计算
为了真实准确的为用户进行实时推荐，还是要依赖历史数据，需要依赖一套完整的离线推荐系统作为数据支撑。因此需要使用离线计算中的模型，来计算物品之间的相似度。由离线推荐部分可知，用户-电影评分矩阵经过ALS算法分解后，将得到两个矩阵，分别为用户-隐含因子矩阵以及隐含因子-电影矩阵。隐含因子-电影矩阵的每一列就可以看作每部电影的隐含向量，使用余弦相似度计算电影两两之间的相似度，并取与每部电影最相似的K部电影存储到数据库中，作为实时推荐的依据。

```scala
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
      // 获取与电影567最相似的10部电影 
val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
      sortedSims = sortedSims.union(sims.sortBy(_._2,false).take(10).map(each => (movieId, each._1, each._2)))
    }

    import sqlContext.implicits._
    val simMovieDF = sc.parallelize(sortedSims.map(each => SimMovie(each._1, each._2, each._3))).toDF()

    //simMovieDF.filter(df() = )
    simMovieDF.show()
    simMovieDF.write.jdbc(url,"simMovies",properties)
```

（2）用户实时行为记录
步骤一 web、wap通过埋点实时发送用户行为数据至后端server， app直接调用http接口，server通过logback直接输出日志文件 
步骤二 flume通过tail命令监控日志文件变化 
步骤三 flume通过生产者消费者模式将tail收集到日志推送至kafka集群 
步骤四 kafka根据服务分配topic，一个topic可以分配多个group，一个group可以分配多个partition 
步骤五 SparkStreaming实时监听kafka，流式处理日志内容，根据特定业务规则，将数据实时存储至数据库，同时根据需要可以写入hdfs 

（3）sparkstreaming流处理
此处将实现实时推荐系统的业务逻辑。Spark Streaming每隔5分钟接受来自Kafka、Flume的用户行为日志，包括用户ID、电影ID以及评分。在每个流处理中，使用Spark SQL，利用聚合找出每位用户在5分钟内其评价的所有电影中获得最高分的那一部电影，再找到与其最相似的一部电影，赋予较高的权值，保存到数据库推荐表中。

```scala
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
```

## Web Server

1.推荐系统API服务

![avatar](http://p35hkafb9.bkt.clouddn.com/API%E9%83%A8%E5%88%86%E7%9B%AE%E5%BD%95.bmp)

使用API服务实现前后端分离架构，我们需要首先确定返回的JSON响应结构是统一的，也就是说，每个请求将返回相同结构的JSON响应结构。不妨定义一个相对通用的JSON响应结构，其中包含两部分：元数据与返回值，其中，元数据表示操作是否成功与返回值消息等，返回值对应服务端方法所返回的数据。该JSON响应结构如下：

```json
{
    "status": 0,
    "msg": "……"，
    "data": {……}
}
```

详情请见《烂豆瓣API说明书》https://github.com/wangj1106/brokendouban/blob/final/README.md

2.Web App
使用ASP.NET MVC构建web应用，利用bootstrap、angularJS渲染页面并与后台交互。

![avatar](http://p35hkafb9.bkt.clouddn.com/web%E5%BA%94%E7%94%A8%E9%83%A8%E5%88%86%E6%88%AA%E5%9B%BE.bmp)

## 工具
Scala、Python、Java
Spark、springboot、.net、mysql

## 快速开始
1.	环境搭建
推荐引擎需要环境包括Scala2.11.8、spark2.2.0、flume-ng-1.6.0-cdh5.7.0、kafka_2.11、zookeeper-3.4.5-cdh5.7.0，API服务需要java1.8。

2.	推荐引擎启动

启动zookeeper：
```
/home/hadoop/app/zookeeper-3.4.5-cdh5.7.0/bin $ ./zkServer.sh start
```
启动Kafka Server：
```
./kafka-server-start.sh -daemon /home/hadoop/app/kafka_2.11-0.9.0.0/config/server.properties
```
修改Flume配置文件使得flume sink数据到kafka

```
streaming_project2.conf
exec-memory-kafka.sources = exec-source
exec-memory-kafka.sinks = kafka-sink
exec-memory-kafka.channels = memory-channel                   
exec-memory-kafka.sources.exec-source.type = exec
exec-memory-kafka.sources.exec-source.command = tail -F /home/hadoop/data/log/userlog.log
exec-memory-kafka.sources.exec-source.shell = /bin/sh -c
exec-memory-kafka.channels.memory-channel.type = memory
exec-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
exec-memory-kafka.sinks.kafka-sink.brokerList = hadoop000:9092
exec-memory-kafka.sinks.kafka-sink.topic = streamingtopic
exec-memory-kafka.sinks.kafka-sink.batchSize = 5
exec-memory-kafka.sinks.kafka-sink.requiredAcks = 1
exec-memory-kafka.sources.exec-source.channels = memory-channel
exec-memory-kafka.sinks.kafka-sink.channel = memory-channel
```

启动
```
flume-ng agent --name exec-memory-kafka --conf $FLUME_HOME/conf --conf-file /home/hadoop/project/streaming_project2.conf -Dflume.root.logger=INFO,console

kafka-console-consumer.sh --zookeeper hadoop000:2181 --topic streamingtopic
```

利用spark-submit提交任务，目录下sparkRecommend.jar。
```
./spark-submit  --class SparkSQLTest --master spark:hadoop1:7077 --executor-memory 2g --num-executors 3  sparkRecommend.jar
```

3.	API服务启动
命令行中运行brokendouban.jar，目录下brokendouban.jar。API说明详见https://github.com/wangj1106/brokendouban/blob/final/README.md
```
java -j brokendouban.jar
```



