package com.zqykj.bigdata.spark.alert.streaming

import com.zqykj.bigdata.spark.alert.kafka.MyKafkaProducer
import com.zqykj.bigdata.spark.alert.rdd.WarningRDD
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by alfer on 7/27/17.
  */
class InOutStreamingExecutor(val sc: SparkContext) {

  def executor(): Unit = {

    val sparkConf = sc.getConf
    val ssc = new StreamingContext(sc, Seconds(sparkConf.getInt("spark.stream.kafka.inout.batch.duration", 2)))
    // brokers: kafka的broker 地址， topics: kafka订阅主题
    val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.stream.warning.brokers", "dev62:9092"),
      sparkConf.get("spark.kafka.stream.warning.topics", "inout"))
    val topicsSet = topics.split(",").toSet.filter(f => f.startsWith("in"))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      // "auto.offset.reset" -> sparkConf.get("spark.kafka.stream.warning.auto.offset.reset", "largest"),
      "group.id" -> sparkConf.get("spark.kafka.stream.warning.group.id", "inout3")
    )

    println(s"brokers=${brokers} , topicsSet=${topicsSet}")
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // 构造 kafka producer 参数
    val kafkaProParams = Map[String, String](
      "bootstrap.servers" -> sparkConf.get("spark.warning.type.kafka.brokers", "dev62:9092"),
      "client.id" -> sparkConf.get("spark.warning.type.kafka.client.id", "GatherOutProducer2"),
      "key.serializer" -> sparkConf.get("spark.warning.type.kafka.key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"),
      "value.serializer" -> sparkConf.get("spark.warning.type.kafka.value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
    )
    val warningTopicSet = sparkConf.get("spark.warning.type.kafka.congest.topics", "inoutAreaWarning")
      .split(",").toSet.filter(f => f.startsWith("inout"))

    messages.foreachRDD {
      rdd => {
        if (!rdd.isEmpty()) {
          // 消息处理 TODO
          processRdd(rdd, kafkaProParams, warningTopicSet)

          // 更新offsets
          //km.updateZKOffsets(rdd)
        }
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

  def processRdd(rdd: RDD[(String, String)], kafkaProParams: Map[String, String], topicSet: Set[String]): Unit = {
    val jsonRDD = WarningRDD.parseJson(rdd)
    println("jsonRDD=" + jsonRDD.first())
    println("partitions.length=" + jsonRDD.partitions.length)
    jsonRDD.foreachPartition(p => {
      MyKafkaProducer.setkafkaParams(kafkaProParams)
      p.foreach {
        line => {
          // println("line=" + line)
          // println("topic=" + topicSet.head + " ,key=" + " ,line=" + line.toString)
          MyKafkaProducer.send(topicSet.head, "key", line.toString)
        }
      }
    }
    )
  }

}
