package com.zqykj.bigdata.spark.alert.streaming

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.{JSON, JSONException}
import com.zqykj.bigdata.alert.entity.{CongestWarningEvent, DetectedData, UFlag}
import com.zqykj.bigdata.alert.util.DateUtils
import com.zqykj.bigdata.spark.LoggerLevels
import com.zqykj.bigdata.spark.alert.kafka.MyKafkaProducer
import com.zqykj.bigdata.spark.alert.rdd.WarningRDD
import com.zqykj.bigdata.spark.alert.redis.RedisUtils
import com.zqykj.job.geo.utils.GeoHash
import kafka.serializer.StringDecoder
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisDataException}

/**
  * 聚集预警
  * Created by weifeng on 2017/6/7.
  */
object GatherWarning extends Logging {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("gather warning")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") // 消息消费完成后，优雅的关闭spark streaming
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
    //.set("spark.streaming.kafka.maxRatePerPartition", "10") // Direct 方式：从每个Kafka分区读取数据的最大速率（每秒记录数）
    //			.setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Milliseconds(sparkConf.getInt("spark.stream.kafka.batch.millis.duration", 2000)))

    // brokers: kafka的broker 地址， topics: kafka订阅主题
    val Array(brokers, topics) = Array(sparkConf.get("spark.kafka.stream.warning.brokers", "Master:9092,Work01:9092,Work03:9092"),
      sparkConf.get("spark.kafka.stream.warning.topics", "gather2"))
    val topicsSet = topics.split(",").toSet

    // 构造 streaming integrate kafka 参数
    println("brokers=" + brokers + " ,topic=" + topics)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      // "auto.offset.reset" -> sparkConf.get("spark.kafka.stream.warning.auto.offset.reset", "largest"),
      "group.id" -> sparkConf.get("spark.kafka.stream.warning.group.id", "cluster3")
    )

    // 构造 kafka producer 参数
    val kafkaProParams = Map[String, String](
      "bootstrap.servers" -> sparkConf.get("spark.warning.type.kafka.brokers", "Master:9092,Work01:9092,Work03:9092"),
      "client.id" -> sparkConf.get("spark.warning.type.kafka.client.id", "GatherOutProducer2"),
      "key.serializer" -> sparkConf.get("spark.warning.type.kafka.key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"),
      "value.serializer" -> sparkConf.get("spark.warning.type.kafka.value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
    )


    val warningTopicSet = sparkConf.get("spark.warning.type.kafka.congest.topics", "congestS")
      .split(",").toSet.filter(f => f.startsWith("gather"))

    // kafka直连方式： 指定topic, 从指定的offset处开始消费
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    //.repartition(sparkConf.getInt("spark.warning.process.partition.num", 2))

    // val dataObjs = parseJson(messages)
    // compare(dataObjs, kafkaProParams, topicSet)

    messages.foreachRDD {
      rdd => {
        if (!rdd.isEmpty()) {
          // 消息处理
          processRdd(rdd, kafkaProParams, warningTopicSet)

          // 更新offsets
          //km.updateZKOffsets(rdd)
        }
      }
    }

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

  def processRdd(rdd: RDD[(String, String)], kafkaProParams: Map[String, String], topicSet: Set[String]): Unit = {
    val jsonRDD = WarningRDD.parseJson(rdd)
    jsonRDD.foreachPartition(p => {
      MyKafkaProducer.setkafkaParams(kafkaProParams)
      p.foreach {
        line => {
          println("update redis...")
          updateRedis(line, topicSet)
        }
      }
    }
    )
  }

  def compare(dstream: DStream[DetectedData], kafkaProParams: Map[String, String], topicSet: Set[String]): Unit = {
    dstream.foreachRDD {
      rdd => {
        rdd.foreachPartition(p => {
          MyKafkaProducer.setkafkaParams(kafkaProParams)
          p.foreach {
            line => {
              println("update redis...")
              updateRedis(line, topicSet)
            }
          }
        }
        )
      }
    }
  }

  def updateRedis(line: DetectedData, topicSet: Set[String]): Unit = {
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
    try {
      val redisArrString = RedisUtils.hGet(hKey, entityType)
      val t4 = System.currentTimeMillis()
      println("query redis time=" + (t4 - t3))
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

      val reList = JSON.parseArray(redisArrString, classOf[UFlag])
        .filter(u => DateUtils.compare(u.getTimestamp, -1))

      // 如果uid已经存在，则返回
      if (reList.map(m => m.getUid).contains(uFlag.getUid)) return
      reList.add(uFlag)
      val converList = bufferAsJavaList(reList)
      // 更新redis
      RedisUtils.insert(hKey, entityType, converList)
      val t8 = System.currentTimeMillis()
      println("check and update redis time=" + (t8 - t7))

      // 检查是否超过5个， 将超出的结果输出
      if (reList.size <= 5) return else if (reList.size < 15000) {
        sendToKafka(line, reList, geoHash, topicSet)
      } else {
        // TODO 以日志形式存储
        println(s"uFlag 数量过多, size = ${reList.size}")
      }

    } catch {
      case jsonEx: JSONException => {
        println(s"JSON parse Exception: ${jsonEx.getMessage}")
      }
      case castEx: ClassCastException => {
        println(s"ClassCastException: ${castEx.getMessage}")
      }
      case jedisDataEx: JedisDataException => {
        println(s"JedisDataException: ${jedisDataEx.getMessage}")
      }
      case jedisEx: JedisConnectionException => {
        println(s"JedisConnectionException: ${jedisEx.getMessage}")
      }
    }

  }

  def sendToKafka(entity: DetectedData, list: util.List[UFlag], geoHash: String, topicSet: Set[String]): Unit = {
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
    MyKafkaProducer.send(topicSet.head, "", event.toString, true)
    val t2 = System.currentTimeMillis()
    println("send msg to kafka time=" + (t2 - t1))
  }

}
