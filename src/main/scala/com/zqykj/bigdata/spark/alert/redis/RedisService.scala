package com.zqykj.bigdata.spark.alert.redis

import java.util

import com.alibaba.fastjson.JSONArray
import com.zqykj.bigdata.alert.entity.UFlag
import com.zqykj.bigdata.alert.util.RedisProvider

/**
  * Created by weifeng on 2017/6/8.
  */
object RedisService {

  def hGet(hKey: String, entityType: String): String = {
    RedisProvider.hGet(hKey, entityType)
  }

  def insert(hkey: String, entityType: String, list: util.List[UFlag]): Unit = {
    val jsonArray = new JSONArray()
    import scala.collection.JavaConversions._
    for (u <- list) {
      jsonArray.add(u)
    }
    val map = new util.HashMap[String, String]()
    map.put(entityType, jsonArray.toJSONString)
    RedisProvider.hmSet(hkey, map)
  }

  def insert(hKey: String, entityType: String, uFlag: UFlag): Unit = {
    val jsonArray = new JSONArray()
    jsonArray.add(uFlag)
    val map = new util.HashMap[String, String]()
    map.put(entityType, jsonArray.toJSONString)
    RedisProvider.hmSet(hKey, map)
  }
}
