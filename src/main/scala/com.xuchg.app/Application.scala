package com.xuchg.app

import com.xuchg.app.listener.AppListener
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application extends App{

  //spark读取kafka示例
  Logger.getLogger("org").setLevel(Level.ERROR)
  val IP = "192.168.20.133"
  val kafkaAddress = IP + ":9092"
  val zookeeper = IP + ":2181"
  val topic = "realtime0103"
  val topicOffset = "{\""+topic+"\":{\"0\":0,\"1\":0,\"2\":0}}"
  val sparkSession = SparkSession
    .builder()
    .config(new SparkConf()
      .setMaster("local[2]")
      .set("spark.streaming.stopGracefullyOnShutdown","true")//设置spark，关掉sparkstreaming程序，并不会立即停止，而是会把当前的批处理里面的数据处理完毕后 才会停掉，此间sparkstreaming不会再消费kafka的数据，这样以来就能保证结果不丢和重复。
      .set("spark.submit.deployMode","cluster")
      .set("spark.executor.memory","4g")//worker内存
      .set("spark.driver.memory","4g")
      .set("spark.cores.max","2")//设置最大核心数
    )
    .appName(getClass.getName)
    .getOrCreate()

  def createStreamDF(spark:SparkSession):DataFrame = {
    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaAddress)
      .option("zookeeper.connect", zookeeper)
      .option("subscribe", topic)
      .option("startingOffsets", topicOffset)
      .option("enable.auto.commit", "false")
      .option("failOnDataLoss", false)
      .option("includeTimestamp", true)
      .load()
    df
  }

  var df = createStreamDF(sparkSession)

  val query = df.writeStream
    .format("console")
    .start()

  //监控spark停止
  sparkSession.sparkContext.addSparkListener(new AppListener)
  //监控spark作业，可以用来检测kafka的offset的变化
  sparkSession.streams.addListener(new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      println("Query started: " + queryStarted.id)
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      //服务出现问题而停止
      println("Query terminated: " + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      var progress = queryProgress.progress
      var sources = progress.sources
      if(sources.length>0){
        var a = 0
        for(a <- 0 to sources.length - 1){
          var offsetStr = sources.apply(a).startOffset
          if(offsetStr!=null){
            println("检测offset是否变化 -- " + offsetStr)
          }
        }
      }

    }
  })

  query.awaitTermination()

}
