package com.zqykj.bigdata

import java.util

import com.alibaba.fastjson.JSONArray._
import com.zqykj.bigdata.alert.util.RedisProvider._

/**
  * Created by weifeng on 2017/6/8.
  */
object GatherTest {

  def main(args: Array[String]): Unit = {
    val elements = hmGet("geohash@testELP", "testCase2-1")
    println(Option(elements.get(0)).isEmpty)
    import scala.collection.JavaConversions._
    for (el <- elements) {
      println(el)
    }
    println("================")
    val value = hGet("geohash@testELP", "testCase2-3")
    println(value)
    if (!Option(value).isEmpty) "" else print(false)
  }


  def foreachList(list: util.List[String]): Unit = {
    println(list.size)
    if (list != null && list.get(0) != null) {
      import scala.collection.JavaConversions._
      for (s <- list) {
        println(s)
      }
    }
  }

}
