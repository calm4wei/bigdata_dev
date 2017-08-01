package com.zqykj.bigdata.spark.streaming

import com.zqykj.bigdata.spark.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, KafkaUtils, OffsetRange}

/**
  * Created by fengwei on 17/5/6.
  */
object SparkStreamingDemo01 {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("early warning")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // brokers:kafka的broker 地址， topics: kafka订阅主题
    val Array(brokers, topics) = Array("dev62:9092", "test")
    val topicsSet = topics.split(",").toSet

    // 构造kafka参数
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      // "auto.offset.reset" -> "largest",
      "group.id" -> "cluster1")

    // kafka直连方式： 指定topic，从指定的offset处开始消费
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // messages.map(record => (record._1, record._2)).print()
    //    messages.foreachRDD(rdd =>
    //      rdd.foreachPartition(p => {
    //        p.foreach(
    //          f => {
    //            println(s"offset=${f._1} , value=${f._2}")
    //          }
    //        )
    //      }
    //      )
    //    )

    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
