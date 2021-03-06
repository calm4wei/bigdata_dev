package com.zqykj.bigdata.spark.alert.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by alfer on 7/27/17.
  */
class SqlExecutor(val sc: SparkContext) {

  val sqlContext = new SQLContext(sc)

  def executor(): DataFrame = {
    val df = findMongo()
    //    val jsonRDD = df.toJSON
    //    println(jsonRDD.first())
    println(df.first())
    df
  }

  def findMongo(): DataFrame = {
    val conf = sc.getConf
    val options = Map[String, String](
      "host" -> conf.get("spark.streaming.mongodb.host", "dev60"),
      "database" -> conf.get("spark.streaming.mongodb.db", "alert"),
      "collection" -> conf.get("spark.streaming.mongodb.collection", "MonitorArea")
    )
    val df = sqlContext.read.format("com.stratio.datasource.mongodb").options(options).load
    df.select("in2OutCheck", "residentDurationThreashhold", "areaId", "shapes")
  }


}
