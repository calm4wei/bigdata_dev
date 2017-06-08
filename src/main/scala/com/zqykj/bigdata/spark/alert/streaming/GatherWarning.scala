package com.zqykj.bigdata.spark.alert.streaming

import java.util

import com.alibaba.fastjson.{JSON, JSONArray}
import com.zqykj.bigdata.alert.entity.{DetectedData, UFlag}
import com.zqykj.bigdata.alert.util.RedisProvider._
import com.zqykj.bigdata.spark.LoggerLevels
import com.zqykj.job.geo.utils.GeoHash
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 聚集预警
  * Created by weifeng on 2017/6/7.
  */
object GatherWarning {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("gather warning")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // brokers:kafka的broker 地址， topics: kafka订阅主题
    val Array(brokers, topics) = Array("wf-vm:9092", "detected")
    val topicsSet = topics.split(",").toSet

    // 构造kafka参数
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest",
      "group.id" -> "cluster1")

    // streaming 接收 kafka 的消息
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      .map(_._2)

    val dataObjs = parseJson(messages)
    compare(dataObjs)

    //    dataObjs.foreachRDD(rdd =>
    //      rdd.foreach {
    //        print("============")
    //        println(_)
    //      }
    //    )

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
              updateRedis(line)
            }
          }
        )
      }
    }
  }

  def updateRedis(line: DetectedData): Unit = {
    val geoHash = GeoHash.encodeGeohash(line.getLatitude.toDouble, line.getLongitude.toDouble, 5)
    val hKey = geoHash + "@" + line.getElpID
    val entityType = line.getEntity_type
    val uFlag = new UFlag(line.getUid, line.getTimestamp)
    println("hKey=" + hKey + " ,type=" + entityType + " ,uFlag=" + uFlag)

    // 查询 redis
    val redisArrString = hGet(hKey, entityType)
    // 不存在则直接插入
    if (Option(redisArrString).isEmpty) insert(hKey, entityType, uFlag) else return

    // 检查有没有过期的数据
    println("check...")

    // 更新redis

    // 检查是否超过5个， 将超出的结果返回


  }

  def insert(hKey: String, entityType: String, uFlag: UFlag): Unit = {
    val jsonArray = new JSONArray()
    jsonArray.add(uFlag)
    val map = new util.HashMap[String, String]()
    map.put(entityType, jsonArray.toJSONString)
    hmSet(hKey, map)
  }


}
