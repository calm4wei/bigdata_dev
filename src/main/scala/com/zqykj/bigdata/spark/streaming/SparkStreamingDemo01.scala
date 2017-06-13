package com.zqykj.bigdata.spark.streaming

import com.zqykj.bigdata.spark.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

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
		val Array(brokers, topics) = Array("Master:9092,Work01:9092,Work03:9092", "detected")
		val topicsSet = topics.split(",").toSet

		// 构造kafka参数
		val kafkaParams = Map[String, String](
			"metadata.broker.list" -> brokers,
			"auto.offset.reset" -> "largest",
			"group.id" -> "cluster1")

		// streaming 接收 kafka 的消息
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
			.map(_._2)
		ssc.checkpoint("checkpoint")
		// TODO 业务处理
		val words = messages.flatMap(_.split(" "))
		println("============>>>" + words)
		val wordCounts = words.map(x => (x, 1))
			.reduceByKey((a, b) => a + b)
		//.reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(2), 2)
		wordCounts.foreachRDD(rdd =>
			rdd.foreach(println(_))
		)
		// 流处理的结果用kafka发送出去
		//Producer.send("")
		// 启动spark streaming
		ssc.start()
		ssc.awaitTermination()
	}

}
