package com.zqykj.bigdata.spark.alert.streaming

import com.zqykj.bigdata.spark.LoggerLevels
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}

/**
  * @author alfer
  * @date 11/15/17
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        LoggerLevels.setStreamingLogLevels(Level.WARN)
        val sparkConf = new SparkConf()
            .setAppName("WordCount")
        //            .set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
        //            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //            .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
        //.set("spark.streaming.kafka.maxRatePerPartition", "10") // Direct 方式：从每个Kafka分区读取数据的最大速率（每秒记录数）

        if (sparkConf.getBoolean("spark.execute.local.model", true))
            sparkConf.setMaster("local[4]").set("spark.ui.port", "4040")

        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(sparkConf.getInt("spark.stream.kafka.batch.second.duration", 2)))
        ssc.checkpoint("checkpoint/baseStation")

        val num = sparkConf.getInt("spark.warning.type.kafka.alert.num", 3)

        // brokers: kafka的broker 地址， topics: kafka订阅主题
        val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.stream.warning.brokers", "bigdatacluster02:9092,bigdatacluster03:9092,bigdatacluster04:9092,bigdatacluster05:9092,bigdatacluster06:9092"),
            sparkConf.get("spark.kafka.stream.warning.topics", "CallRecord2"))
        val topicsSet = topics.split(",").toSet

        // 构造 streaming integrate kafka 参数
        println("brokers=" + brokers + " ,topic=" + topics)
        val kafkaParams = Map[String, String](
            "metadata.broker.list" -> brokers,
            "auto.offset.reset" -> sparkConf.get("spark.kafka.stream.warning.auto.offset.reset", "largest"),
            "group.id" -> sparkConf.get("spark.kafka.stream.warning.group.id", "baseConsumer2")
        )

        // 构造 kafka producer 参数
        val kafkaProParams = Map[String, String](
            "bootstrap.servers" -> sparkConf.get("spark.warning.type.kafka.brokers", "bigdatacluster02:9092,bigdatacluster03:9092,bigdatacluster04:9092,bigdatacluster05:9092,bigdatacluster06:9092"),
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

        // 记录kafka消费的消息偏移量
        var offsetRanges = Array[OffsetRange]()
        val rdds = messages.transform { rdd =>
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        rdds.foreachRDD(rdd => {
            val pairs = rdd.flatMap(_._2.split(",")).map(word => (word, 1))
            val wordCounts = pairs.reduceByKey(_ + _)
            wordCounts.foreachPartition(p => {
                p.foreach(f => {
                    print(f)
                })
                // 更新offsets
                for (o <- offsetRanges) {
                    println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
                    km.updateZKOffsets(o)
                }
            })

        })

        ssc.start()
        ssc.awaitTermination()

    }

}
