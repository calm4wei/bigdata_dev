package com.zqykj.bigdata.spark.alert.streaming

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.{JSON, JSONException}
import com.zqykj.bigdata.alert.entity.{CongestWarningEvent, DetectedData, UFlag}
import com.zqykj.bigdata.alert.util.DateUtils
import com.zqykj.bigdata.spark.LoggerLevels
import com.zqykj.bigdata.spark.alert.kafka.KafkaProducer
import com.zqykj.bigdata.spark.alert.redis.RedisUtils
import com.zqykj.job.geo.utils.GeoHash
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 聚集预警
  * Created by weifeng on 2017/6/7.
  */
object GatherWarning extends Logging {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("gather warning")
      // .set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.kafka.maxRatePerPartition", "10") // Direct 方式：从每个Kafka分区读取数据的最大速率（每秒记录数）
    //      .setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // brokers:kafka的broker 地址， topics: kafka订阅主题
    val Array(brokers, topics) = Array(sparkConf.get("kafka.stream.warning.brokers", "Master:9092,Work01:9092,Work03:9092"),
      sparkConf.get("kafka.stream.warning.topic", "gather"))
    val topicsSet = topics.split(",").toSet

    // 构造kafka参数
    println("brokers=" + brokers + " ,topic=" + topics)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> sparkConf.get("kafka.stream.warning.auto.offset.reset", "largest"),
      "group.id" -> sparkConf.get("kafka.stream.warning.group.id", "cluster1")
    )

    // kafka直连方式： 指定topic，从指定的offset处开始消费
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    val dataObjs = parseJson(messages)
    compare(dataObjs)

    ssc.start()
    ssc.awaitTermination()


    // Spark 1.4版本之前: 通过添加钩子来优雅关闭，保证Streaming程序关闭的时候不丢失数据
    // Spark 1.4版本之后： Spark内置提供了spark.streaming.stopGracefullyOnShutdown参数来决定是否需要以Gracefully方式来关闭Streaming程序
    //    sys.addShutdownHook {
    //      println("Gracefully stopping Application...")
    //      ssc.stop(stopSparkContext = true, stopGracefully = true)
    //      // wait something
    //      println("Application stopped gracefully")
    //    }

  }

  def parseJson(dstream: DStream[String]): DStream[DetectedData] = {
    dstream.map(line => {
      JSON.parseObject(line, classOf[DetectedData])
    })
  }

  def compare(dstream: DStream[DetectedData]): Unit = {
    dstream.foreachRDD {
      rdd => {
        rdd.foreachPartition(p => {
          // val list = p.toList
          p.foreach {
            line => {
              println("update redis...")
              updateRedis(line)
            }
          }
        }
        )
      }
    }
  }

  def updateRedis(line: DetectedData): Unit = {
    val t1 = System.currentTimeMillis()
    val geoHash = GeoHash.encodeGeohash(line.getLatitude.toDouble, line.getLongitude.toDouble, 5)
    val t2 = System.currentTimeMillis()
    println(s"compute geohash time=${t2 - t1}")

    val hKey = geoHash + "@" + line.getElpID
    val entityType = line.getEntity_type
    val uFlag = new UFlag(line.getUid, line.getTimestamp)
    println("hKey=" + hKey + " ,type=" + entityType + " ,uFlag=" + uFlag)

    val t3 = System.currentTimeMillis()
    // 查询 redis
    val redisArrString = RedisUtils.hGet(hKey, entityType)
    val t4 = System.currentTimeMillis()
    println("query redis time=" + (t4 - t3) + ", redisArrString=" + redisArrString)
    // 不存在则直接插入
    if (Option(redisArrString).isEmpty) {
      val t5 = System.currentTimeMillis()
      RedisUtils.insert(hKey, entityType, uFlag)
      val t6 = System.currentTimeMillis()
      println("insert redis time=" + (t6 - t5))
      return
    }

    // 检查有没有过期的数据
    import scala.collection.JavaConversions._
    val t7 = System.currentTimeMillis()

    try {
      val reList = JSON.parseArray(redisArrString, classOf[UFlag])
        .filter(u => DateUtils.compare(u.getTimestamp, -1))
      val converList = bufferAsJavaList(reList)
      reList.add(uFlag)
      // 更新redis
      RedisUtils.insert(hKey, entityType, converList)
      val t8 = System.currentTimeMillis()
      println("check and update redis time=" + (t8 - t7))

      // 检查是否超过5个， 将超出的结果输出
      if (reList.size <= 5) return else if (reList.size < 15000) {
        sendToKafka(line, reList, geoHash)
      } else {
        // TODO 以日志形式存储
        println(s"uFlag 数量过多, size = ${reList.size}")
      }

    } catch {
      case ex: JSONException => {
        println("JSON parse Exception:" + ex.getMessage)
      }
    }

  }

  def sendToKafka(entity: DetectedData, list: util.List[UFlag], geoHash: String): Unit = {
    val t1 = System.currentTimeMillis()
    val event = new CongestWarningEvent
    event.setEventId(UUID.randomUUID().toString)
    event.setWarningType("congestS")
    event.setOrigineTime(entity.getTimestamp)
    event.setProduceTime(entity.getTimestamp)
    event.setEventTime(new Date().getTime)
    event.setInElpId(entity.getElpID)
    event.setInTypeId(entity.getEntity_type)
    event.setInUid(entity.getUid)
    event.setInLabel(entity.getLabel)
    event.setGeohashString(geoHash)
    event.setFlagList(list)
    KafkaProducer.send("congestS", "", event.toString, true)
    val t2 = System.currentTimeMillis()
    println("send msg to kafka time=" + (t2 - t1))
  }

}
