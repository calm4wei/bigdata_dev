package com.zqykj.bigdata.spark.alert.streaming

import com.zqykj.bigdata.spark.alert.common.OptionsConstans
import com.zqykj.bigdata.spark.alert.sql.SqlExecutor
import org.apache.commons.cli._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alfer on 7/27/17.
  */
object InOutWarning {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("InOut Warning")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
      .setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlExecutor = new SqlExecutor(sc)
    // TODO 区域与经纬度的数据结构：<lat-lon, areaId>
    val df = sqlExecutor.findMongo()

    val broadcastVar = sc.broadcast()

    val sscExecutor = new InOutStreamingExecutor(sc)
    sscExecutor.executor()

  }

  def getOptions(args: Array[String]): CommandLine = {
    val options = new Options()
    for (arg <- args) {
      println(arg)
    }
    options.addOption(OptionsConstans.opt_mongodb, true, "")
    val parser = new DefaultParser()
    parser.parse(options, args)
  }

}
