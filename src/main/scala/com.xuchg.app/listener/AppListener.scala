package com.xuchg.app.listener

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}

class AppListener extends SparkListener{
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    //监控spark停止方法，可以处理spark结束的动作
    println("application 关闭")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

    println("application 启动")
  }
}
