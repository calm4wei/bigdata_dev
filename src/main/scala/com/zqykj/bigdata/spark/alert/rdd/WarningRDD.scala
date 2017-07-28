package com.zqykj.bigdata.spark.alert.rdd

import com.alibaba.fastjson.JSON
import com.zqykj.bigdata.alert.entity.DetectedData
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by alfer on 7/28/17.
  */
class WarningRDD[K, V](sc: SparkContext) extends RDD[(K, V)](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = ???

  override protected def getPartitions: Array[Partition] = ???
}

object WarningRDD {

  def parseJson(rdd: RDD[(String, String)]): RDD[DetectedData] = {
    rdd.map(line => {
      JSON.parseObject(line._2, classOf[DetectedData])
    })
  }

  def parseJson(dstream: DStream[String]): DStream[DetectedData] = {
    dstream.map(line => {
      JSON.parseObject(line, classOf[DetectedData])
    })
  }

}
