package com.zqykj.bigdata.spark.alert.streaming

import com.alibaba.fastjson.JSON
import com.zqykj.bigdata.alert.entity.CallRecode
import com.zqykj.bigdata.spark.LoggerLevels
import com.zqykj.bigdata.spark.alert.kafka.MyKafkaProducer
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaManager

/**
  * Created by weifeng on 2017/6/21.
  */
object BaseStationWarning {


	//Seq这个批次某个单词的次数
	//Option[Int]：以前的结果

	//分好组的数据
	val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
		//iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
		//iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
		//iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
		iter.map { case (word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
	}

	def main(args: Array[String]): Unit = {
		LoggerLevels.setStreamingLogLevels(Level.WARN)

		val sparkConf = new SparkConf()
			.setAppName("gather warning")
			.set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
		//.set("spark.streaming.kafka.maxRatePerPartition", "10") // Direct 方式：从每个Kafka分区读取数据的最大速率（每秒记录数）
		//.setMaster("local[4]")

		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc, Seconds(sparkConf.getInt("spark.stream.kafka.batch.second.duration", 2)))
		ssc.checkpoint("checkpoint/baseStation")

		// brokers: kafka的broker 地址， topics: kafka订阅主题
		val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.stream.warning.brokers", "wf-vm:9092"),
			sparkConf.get("spark.kafka.stream.warning.topics", "CallRecord"))
		val topicsSet = topics.split(",").toSet

		// 构造 streaming integrate kafka 参数
		println("brokers=" + brokers + " ,topic=" + topics)
		val kafkaParams = Map[String, String](
			"metadata.broker.list" -> brokers,
			// "auto.offset.reset" -> sparkConf.get("spark.kafka.stream.warning.auto.offset.reset", "largest"),
			"group.id" -> sparkConf.get("spark.kafka.stream.warning.group.id", "baseConsumer")
		)

		// 构造 kafka producer 参数
		val kafkaProParams = Map[String, String](
			"bootstrap.servers" -> sparkConf.get("spark.warning.type.kafka.brokers", "wf-vm:9092"),
			"client.id" -> sparkConf.get("spark.warning.type.kafka.client.id", "BaseOutProducer"),
			"key.serializer" -> sparkConf.get("spark.warning.type.kafka.key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer"),
			"value.serializer" -> sparkConf.get("spark.warning.type.kafka.value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer")
		)
		val outTopicSet = sparkConf.get("spark.warning.type.kafka.congest.topics", "Alert").split(",").toSet

		// kafka直连方式： 指定topic，从指定的offset处开始消费
		val km = new KafkaManager(kafkaParams)
		val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		// messages.print(10)

		val crStream = parseJson(messages)
		// 每slide 时间，统计 window 时间内的基站通话数量
		val windowStream = crStream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b),
			Seconds(sparkConf.getInt("spark.warning.process.window.second.duration", 60)),
			Seconds(sparkConf.getInt("spark.warning.process.slide.second.duration", 10)))

		// val updateResult = windowStream.updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
		windowStream.foreachRDD(rdd => {
			rdd.foreachPartition(p => {
				MyKafkaProducer.setkafkaParams(kafkaProParams)
				p.foreach(station => {
					if (station._2 > 3) {
						// println(s"station warning: ${station.toString()}")
						MyKafkaProducer.send(outTopicSet.head, "", station.toString(), true)
					}
				})
			})
		})

		ssc.start()
		ssc.awaitTermination()

	}

	def parseJson(rdd: DStream[(String, String)]): DStream[(String, Int)] = {
		rdd.map(line => {
			// TODO json 解析直接映射到实体类 和 解析成jsonObject性能比较
			val callRecode = JSON.parseObject(line._2, classOf[CallRecode])
			val lacAndci = callRecode.getLac + "_" + callRecode.getCi
			(lacAndci, 1)
		})
	}

}
