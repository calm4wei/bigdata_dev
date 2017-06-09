package com.zqykj.bigdata.spark.alert.streaming

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.JSON
import com.zqykj.bigdata.alert.entity.{CongestWarningEvent, DetectedData, UFlag}
import com.zqykj.bigdata.alert.util.{DateUtils, RedisProvider}
import com.zqykj.bigdata.kafka.Producer
import com.zqykj.bigdata.spark.LoggerLevels
import com.zqykj.job.geo.utils.GeoHash
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 聚集预警
  * Created by weifeng on 2017/6/7.
  */
object GatherWarning extends Logging {

  var producer: Producer = null

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("gather warning")
    //.setMaster("local[4]")
    // 初始化配置
    producer = new Producer(sparkConf.get("kafka.stream.warning.type.topic", "congestS"))
    RedisProvider.initRedisContext(sparkConf)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // brokers:kafka的broker 地址， topics: kafka订阅主题
    val Array(brokers, topics) = Array(sparkConf.get("kafka.stream.warning.brokers", "wf-vm:9092"),
      sparkConf.get("kafka.stream.warning.topic", "detected"))
    val topicsSet = topics.split(",").toSet

    // 构造kafka参数
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> sparkConf.get("kafka.stream.warning.auto.offset.reset", "largest"),
      "group.id" -> sparkConf.get("kafka.stream.warning.group.id", "cluster1"))

    // streaming 接收 kafka 的消息
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      .map(_._2)

    val dataObjs = parseJson(messages)
    compare(dataObjs)

    ssc.start()
    ssc.awaitTermination()
  }

  def parseJson(dstream: DStream[String]): DStream[DetectedData] = {
    dstream.map(line => {
      JSON.parseObject(line, classOf[DetectedData])
    })
  }

  def compare(dstream: DStream[DetectedData]): Unit = {
    dstream.foreachRDD {
      rdd => {
        rdd.foreachPartition(p =>
          p.foreach {
            line => {
              logInfo("update redis...")
              updateRedis(line)
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
    logInfo("compute geohash time=" + (t2 - t1))

    val hKey = geoHash + "@" + line.getElpID
    val entityType = line.getEntity_type
    val uFlag = new UFlag(line.getUid, line.getTimestamp)
    logInfo("hKey=" + hKey + " ,type=" + entityType + " ,uFlag=" + uFlag)

    import com.zqykj.bigdata.spark.alert.redis.RedisService._
    val t3 = System.currentTimeMillis()
    // 查询 redis
    val redisArrString = hGet(hKey, entityType)
    val t4 = System.currentTimeMillis()
    logInfo("query redis time=" + (t4 - t3) + ", redisArrString=" + redisArrString)
    // 不存在则直接插入
    if (Option(redisArrString).isEmpty) {
      val t5 = System.currentTimeMillis()
      insert(hKey, entityType, uFlag)
      val t6 = System.currentTimeMillis()
      logInfo("insert redis time=" + (t6 - t5))
      return
    }

    // 检查有没有过期的数据
    import scala.collection.JavaConversions._
    val t7 = System.currentTimeMillis()
    val reList = JSON.parseArray(redisArrString, classOf[UFlag])
      .filter(u => DateUtils.compare(u.getTimestamp, -1))

    val converList = bufferAsJavaList(reList)
    reList.add(uFlag)
    // 更新redis
    insert(hKey, entityType, converList)
    val t8 = System.currentTimeMillis()
    logInfo("check and update redis time=" + (t8 - t7))

    // 检查是否超过5个， 将超出的结果输出
    if (reList.size <= 5) return else sendToKafka(line, reList, geoHash)

  }

  def sendToKafka(entity: DetectedData, list: util.List[UFlag], geoHash: String): Unit = {
    val t1 = System.currentTimeMillis()
    val event = new CongestWarningEvent
    event.setEventId(UUID.randomUUID().toString)
    event.setWarningType(producer.getTopic)
    event.setOrigineTime(entity.getTimestamp)
    event.setProduceTime(entity.getTimestamp)
    event.setEventTime(new Date().getTime)
    event.setInElpId(entity.getElpID)
    event.setInTypeId(entity.getEntity_type)
    event.setInUid(entity.getUid)
    event.setInLabel(entity.getLabel)
    event.setGeohashString(geoHash)
    event.setFlagList(list)
    producer.send("", event.toString)
    val t2 = System.currentTimeMillis()
    logInfo("send msg to kafka time=" + (t2 - t1))
  }


}
