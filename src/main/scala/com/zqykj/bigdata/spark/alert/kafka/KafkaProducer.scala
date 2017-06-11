package com.zqykj.bigdata.spark.alert.kafka

import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.Logging

/**
  * Created by weifeng on 2017/6/9.
  */
object KafkaProducer extends Serializable with Logging {


  private lazy val producer = {
    val props = new Properties
    props.put("bootstrap.servers", "Master:9092,Work01:9092,Work03:9092")
    props.put("client.id", "GatherOutProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def apply(): KafkaProducer[String, String] = producer

  /**
    *
    * @param topic
    * @param key
    * @param value
    * @param isAsync
    */
  def send(topic: String, key: String, value: String, isAsync: Boolean): Unit = {
    if (isAsync) { // 异步发送
      producer.send(new ProducerRecord[String, String](topic, key, value), new Callback() {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          println("topic=" + recordMetadata.topic() + " ,offset=" + recordMetadata.offset())
        }
      })
    } else { // 同步发送
      try {
        producer.send(new ProducerRecord[String, String](topic, key, value)).get
        println("Sent message: (" + key + ", " + value + ")")
      } catch {
        case e@(_: InterruptedException | _: ExecutionException) =>
          e.printStackTrace()
      }
    }
  }

}
